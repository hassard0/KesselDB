//! Multi-node KesselDB: a `kessel-vsr` `Replica` driven over **real TCP
//! sockets** (not the in-process sim bus). One engine thread owns the
//! non-`Send` `Replica<DirVfs>`; everything reaches it as an `Ev` on one
//! channel, so apply stays serial and deterministic. Peer transport is
//! length-prefixed `wire::encode(Msg)` frames; each ordered pair gets one
//! dial→accept link. Loss/disconnect is tolerated (VSR's job), so a writer
//! that can't reach a peer just drops — the protocol re-drives.
//!
//! SP38 scope: the consensus + socket-transport milestone. Clients submit
//! `Op`s (linearized through `Msg::Request`); replies are emitted on the
//! primary, so a client connects to the primary. SQL-over-cluster and
//! cross-node client-reply routing on failover are honest follow-ups.

use kessel_codec::Value;
use kessel_io::DirVfs;
use kessel_proto::wire::{read_frame, write_frame};
use kessel_proto::{ClientId, ObjectId, Op, OpResult};
use kessel_sm::StateMachine;
use kessel_sql::Stmt;
use kessel_vsr::{wire, Msg, Replica};
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender};
use std::sync::Arc;
use std::time::Duration;

/// One tick every this often drives heartbeats / view-change timers.
const TICK_MS: u64 = 12;

enum Ev {
    Client { client: ClientId, req: u64, op: Op, reply: SyncSender<OpResult> },
    /// A raw client frame (`Op::encode()` or `[0xFE] ++ SQL`). SQL must be
    /// compiled on the engine thread because it needs the live catalog,
    /// which is owned by the non-`Send` `Replica`.
    ClientRaw { frame: Vec<u8>, reply: SyncSender<OpResult> },
    Peer { from: usize, msg: Msg },
    Tick,
    Probe(SyncSender<(u32, u64, u64)>),
}

/// What to do when a linearized op's result comes back.
enum Cont {
    /// Forward the result straight to the waiting caller.
    Reply(SyncSender<OpResult>),
    /// SQL `UPDATE` read-modify-write: the just-returned `GetById` record is
    /// patched with `sets`, then re-submitted as a single replicated
    /// `Op::Update`, whose result goes to `reply`.
    Update {
        type_id: u32,
        id: u128,
        sets: Vec<(u16, Value)>,
        reply: SyncSender<OpResult>,
    },
}

/// Patch `rec` (a decoded record) with `sets` and re-encode it into an
/// `Op::Update`. Pure; runs on the engine thread against the live catalog.
fn build_update(
    cat: &kessel_catalog::Catalog,
    type_id: u32,
    id: u128,
    rec: &[u8],
    sets: &[(u16, Value)],
) -> Result<Op, String> {
    let ot = cat
        .get(type_id)
        .ok_or_else(|| "update: no type".to_string())?
        .clone();
    let mut vals =
        kessel_codec::decode(&ot, rec).map_err(|e| format!("update decode: {e:?}"))?;
    for (fid, v) in sets {
        if let Some(i) = ot.fields.iter().position(|f| f.field_id == *fid) {
            vals[i] = v.clone();
        }
    }
    let record =
        kessel_codec::encode(&ot, &vals).map_err(|e| format!("update encode: {e:?}"))?;
    Ok(Op::Update { type_id, id: ObjectId::from_u128(id), record })
}

/// A running node. Holds the engine channel; `submit` linearizes an op
/// through VSR and blocks for the committed reply.
pub struct Node {
    tx: Sender<Ev>,
    client_seq: Arc<AtomicU64>,
    session_seq: Arc<AtomicU64>,
}

/// A stable client session: one VSR `ClientId` plus a monotonic request
/// counter. This is what makes retries **exactly-once** — re-submitting the
/// same `(client, req)` (e.g. a client that timed out and retried) is
/// deduped by the replica's client table and returns the *cached* result
/// without re-applying. Without a stable id (as bare `submit` uses) every
/// call is a new client and a retry would double-apply.
pub struct Session {
    tx: Sender<Ev>,
    client: ClientId,
    req: AtomicU64,
}

impl Session {
    /// Submit `op` under the next request number; blocks for the result.
    pub fn submit(&self, op: Op) -> OpResult {
        let req = self.req.fetch_add(1, Ordering::Relaxed) + 1;
        self.submit_with_req(op, req)
    }

    /// This session's stable VSR client id (so a failover client can retry
    /// the same `(client, req)` against another node via `Node::submit_as`).
    pub fn client_id(&self) -> ClientId {
        self.client
    }

    /// Submit `op` under an explicit request number. Re-using a number that
    /// already committed is a *retry*: the replica returns the cached reply
    /// and does not execute the op again (exactly-once).
    pub fn submit_with_req(&self, op: Op, req: u64) -> OpResult {
        let (rtx, rrx) = sync_channel(1);
        if self
            .tx
            .send(Ev::Client { client: self.client, req, op, reply: rtx })
            .is_err()
        {
            return OpResult::SchemaError("engine stopped".into());
        }
        rrx.recv()
            .unwrap_or_else(|_| OpResult::SchemaError("engine dropped reply".into()))
    }
}

impl Node {
    /// Linearize `op` through consensus and wait for its applied result.
    /// Each call is a fresh VSR client id (req=1) so it is never deduped.
    pub fn submit(&self, op: Op) -> OpResult {
        let client = self.client_seq.fetch_add(1, Ordering::Relaxed) as u128;
        let (rtx, rrx) = sync_channel(1);
        if self
            .tx
            .send(Ev::Client { client, req: 1, op, reply: rtx })
            .is_err()
        {
            return OpResult::SchemaError("engine stopped".into());
        }
        rrx.recv()
            .unwrap_or_else(|_| OpResult::SchemaError("engine dropped reply".into()))
    }

    /// Submit a raw client frame (`Op::encode()` or `[0xFE] ++ SQL`) and
    /// block for the committed result. This is the cluster equivalent of
    /// the single-node `EngineHandle::apply_raw` — full SQL over consensus.
    pub fn apply_raw(&self, frame: Vec<u8>) -> OpResult {
        let (rtx, rrx) = sync_channel(1);
        if self.tx.send(Ev::ClientRaw { frame, reply: rtx }).is_err() {
            return OpResult::SchemaError("engine stopped".into());
        }
        rrx.recv()
            .unwrap_or_else(|_| OpResult::SchemaError("engine dropped reply".into()))
    }

    /// Submit `op` under an explicit `(client, req)` to *this* node. This is
    /// what a failover-aware client uses to retry against a surviving node:
    /// any node holding the committed result answers from its replicated
    /// client table; otherwise a backup relays to the primary.
    pub fn submit_as(&self, client: ClientId, req: u64, op: Op) -> OpResult {
        let (rtx, rrx) = sync_channel(1);
        if self
            .tx
            .send(Ev::Client { client, req, op, reply: rtx })
            .is_err()
        {
            return OpResult::SchemaError("engine stopped".into());
        }
        rrx.recv()
            .unwrap_or_else(|_| OpResult::SchemaError("engine dropped reply".into()))
    }

    /// Open a stable client session (exactly-once retries). The session's
    /// `ClientId` is tagged into a range disjoint from bare `submit`
    /// (small) and internal SQL ops (`1<<100+`).
    pub fn session(&self) -> Session {
        let ord = self.session_seq.fetch_add(1, Ordering::Relaxed) as u128;
        Session {
            tx: self.tx.clone(),
            client: (1u128 << 64) | ord,
            req: AtomicU64::new(0),
        }
    }

    /// `(state digest, op_number, commit)` — for replication assertions.
    pub fn probe(&self) -> (u32, u64, u64) {
        let (ptx, prx) = sync_channel(1);
        if self.tx.send(Ev::Probe(ptx)).is_err() {
            return (0, 0, 0);
        }
        prx.recv().unwrap_or((0, 0, 0))
    }
}

/// Spawn node `self_idx` of an `addrs.len()`-node cluster. `addrs[i]` is
/// node *i*'s peer-listen address; `peer_listener` is our own (already
/// bound) peer socket. The engine opens its own data dir (non-`Send` VFS).
pub fn spawn_node(
    self_idx: usize,
    peer_listener: TcpListener,
    addrs: Vec<SocketAddr>,
    data_dir: PathBuf,
) -> io::Result<Node> {
    let n = addrs.len();
    let (etx, erx) = channel::<Ev>();

    // --- Outbound: one writer thread per peer, lazily (re)dialing. ---
    let mut writers: HashMap<usize, Sender<Vec<u8>>> = HashMap::new();
    for peer in 0..n {
        if peer == self_idx {
            continue;
        }
        let (wtx, wrx) = channel::<Vec<u8>>();
        writers.insert(peer, wtx);
        let paddr = addrs[peer];
        let me = self_idx as u32;
        std::thread::spawn(move || {
            let mut sock: Option<TcpStream> = None;
            while let Ok(bytes) = wrx.recv() {
                if sock.is_none() {
                    if let Ok(mut s) = TcpStream::connect(paddr) {
                        // Announce who we are so the peer tags inbound.
                        if write_frame(&mut s, &me.to_le_bytes()).is_ok() {
                            sock = Some(s);
                        }
                    }
                }
                if let Some(s) = sock.as_mut() {
                    if write_frame(s, &bytes).is_err() {
                        sock = None; // drop; VSR re-drives
                    }
                }
            }
        });
    }

    // --- Inbound: accept links, read sender idx, stream Msgs to engine. ---
    {
        let etx = etx.clone();
        std::thread::spawn(move || {
            for stream in peer_listener.incoming().flatten() {
                let etx = etx.clone();
                std::thread::spawn(move || {
                    let mut s = stream;
                    let hello = match read_frame(&mut s) {
                        Ok(h) if h.len() == 4 => h,
                        _ => return,
                    };
                    let from = u32::from_le_bytes(hello.try_into().unwrap()) as usize;
                    while let Ok(buf) = read_frame(&mut s) {
                        match wire::decode(&buf) {
                            Some(msg) => {
                                if etx.send(Ev::Peer { from, msg }).is_err() {
                                    return;
                                }
                            }
                            None => return,
                        }
                    }
                });
            }
        });
    }

    // --- Heartbeat / timer ticks. ---
    {
        let etx = etx.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(TICK_MS));
            if etx.send(Ev::Tick).is_err() {
                return;
            }
        });
    }

    // --- The single engine thread: sole owner of the Replica. ---
    let (ready_tx, ready_rx) = channel::<io::Result<()>>();
    std::thread::spawn(move || {
        let sm = match DirVfs::new(&data_dir).and_then(StateMachine::open) {
            Ok(sm) => {
                let _ = ready_tx.send(Ok(()));
                sm
            }
            Err(e) => {
                let _ = ready_tx.send(Err(e));
                return;
            }
        };
        let mut replica: Replica<DirVfs> = Replica::new(self_idx, n, sm);
        // (client, req) -> continuation for routing the committed result.
        let mut pending: HashMap<(ClientId, u64), Cont> = HashMap::new();
        // Internal consensus ops (SQL compile / UPDATE RMW) use a client-id
        // range disjoint from external `Node::submit` ids (which start at 1).
        let mut iseq: u128 = 1u128 << 100;
        // SP51: engine-thread-local prepared-statement cache for the
        // cluster SQL path, keyed by `(sql, catalog_epoch)`. The epoch is
        // bumped on every committed catalog change, so a cached plan is
        // never reused against a changed schema (no explicit invalidation
        // needed — a stale-epoch entry is simply recompiled).
        let mut sqlcache: HashMap<String, (u64, Stmt)> = HashMap::new();

        // Drive an `Out` to completion: ship peer msgs, route replies, and
        // chase `Update` continuations (each spawns a follow-up replicated
        // op) until the work queue drains. Sole mutator of `replica`.
        let process = |replica: &mut Replica<DirVfs>,
                        pending: &mut HashMap<(ClientId, u64), Cont>,
                        iseq: &mut u128,
                        first: kessel_vsr::Out| {
            let mut queue = vec![first];
            while let Some(out) = queue.pop() {
                for (to, msg) in out.msgs {
                    if to == self_idx {
                        continue;
                    }
                    if let Some(w) = writers.get(&to) {
                        let _ = w.send(wire::encode(&msg));
                    }
                }
                for (client, req, res) in out.replies {
                    let Some(cont) = pending.remove(&(client, req)) else {
                        continue;
                    };
                    match cont {
                        Cont::Reply(s) => {
                            let _ = s.send(res);
                        }
                        Cont::Update { type_id, id, sets, reply } => match res {
                            OpResult::Got(rec) => {
                                match build_update(
                                    replica.catalog(),
                                    type_id,
                                    id,
                                    &rec,
                                    &sets,
                                ) {
                                    Ok(op) => {
                                        *iseq += 1;
                                        let c = *iseq;
                                        pending
                                            .insert((c, 1), Cont::Reply(reply));
                                        let o2 = replica.handle(
                                            self_idx,
                                            Msg::Request { client: c, req: 1, op },
                                        );
                                        queue.push(o2);
                                    }
                                    Err(e) => {
                                        let _ = reply
                                            .send(OpResult::SchemaError(e));
                                    }
                                }
                            }
                            other => {
                                // NotFound etc. — RMW target absent.
                                let _ = reply.send(other);
                            }
                        },
                    }
                }
            }
        };

        // Submit one op through consensus under a fresh internal client id,
        // with `cont` to fire when it commits. Returns the driven `Out` and
        // the `(client, req)` key, so the caller can redirect if stranded.
        let submit_internal = |replica: &mut Replica<DirVfs>,
                               pending: &mut HashMap<(ClientId, u64), Cont>,
                               iseq: &mut u128,
                               op: Op,
                               cont: Cont| {
            *iseq += 1;
            let c = *iseq;
            pending.insert((c, 1), cont);
            let out = replica.handle(self_idx, Msg::Request { client: c, req: 1, op });
            (out, (c, 1u64))
        };

        // If a request is still pending and this node is NOT the active
        // primary, it will never be answered here (a backup only relays;
        // the reply lands on the primary). Tell the client to try another
        // node — exactly-once (SP40/41) makes the cross-node retry safe.
        let redirect = |replica: &Replica<DirVfs>,
                        pending: &mut HashMap<(ClientId, u64), Cont>,
                        key: (ClientId, u64)| {
            if replica.is_active_primary() {
                return; // primary: the reply arrives async on commit
            }
            if let Some(cont) = pending.remove(&key) {
                let s = match cont {
                    Cont::Reply(s) => s,
                    Cont::Update { reply, .. } => reply,
                };
                let _ = s.send(OpResult::Unavailable);
            }
        };

        while let Ok(ev) = erx.recv() {
            match ev {
                Ev::Client { client, req, op, reply } => {
                    pending.insert((client, req), Cont::Reply(reply));
                    let out =
                        replica.handle(self_idx, Msg::Request { client, req, op });
                    process(&mut replica, &mut pending, &mut iseq, out);
                    redirect(&replica, &mut pending, (client, req));
                }
                Ev::ClientRaw { frame, reply } => {
                    if frame.first() == Some(&0xFE) {
                        let sql = match std::str::from_utf8(&frame[1..]) {
                            Ok(s) => s,
                            Err(_) => {
                                let _ = reply
                                    .send(OpResult::SchemaError("sql: not utf8".into()));
                                continue;
                            }
                        };
                        let epoch = replica.catalog_epoch();
                        let compiled = match sqlcache.get(sql) {
                            Some((e, s)) if *e == epoch => Ok(s.clone()),
                            _ => kessel_sql::compile_stmt(sql, replica.catalog())
                                .map(|s| {
                                    if sqlcache.len() >= 4096 {
                                        sqlcache.clear(); // bounded, deterministic
                                    }
                                    sqlcache
                                        .insert(sql.to_string(), (epoch, s.clone()));
                                    s
                                }),
                        };
                        match compiled {
                            Ok(Stmt::Op(o)) => {
                                let (out, key) = submit_internal(
                                    &mut replica,
                                    &mut pending,
                                    &mut iseq,
                                    o,
                                    Cont::Reply(reply),
                                );
                                process(
                                    &mut replica,
                                    &mut pending,
                                    &mut iseq,
                                    out,
                                );
                                redirect(&replica, &mut pending, key);
                            }
                            Ok(Stmt::Update { type_id, id, sets }) => {
                                // RMW: linearized GetById, then patched Update.
                                let (out, key) = submit_internal(
                                    &mut replica,
                                    &mut pending,
                                    &mut iseq,
                                    Op::GetById {
                                        type_id,
                                        id: ObjectId::from_u128(id),
                                    },
                                    Cont::Update { type_id, id, sets, reply },
                                );
                                process(
                                    &mut replica,
                                    &mut pending,
                                    &mut iseq,
                                    out,
                                );
                                redirect(&replica, &mut pending, key);
                            }
                            Ok(Stmt::Explain(plan)) => {
                                // EXPLAIN: pure planner text, no consensus.
                                let _ = reply
                                    .send(OpResult::Got(plan.into_bytes()));
                            }
                            Err(e) => {
                                let _ = reply
                                    .send(OpResult::SchemaError(format!("sql: {e}")));
                            }
                        }
                    } else {
                        match Op::decode(&frame) {
                            Some(o) => {
                                let (out, key) = submit_internal(
                                    &mut replica,
                                    &mut pending,
                                    &mut iseq,
                                    o,
                                    Cont::Reply(reply),
                                );
                                process(
                                    &mut replica,
                                    &mut pending,
                                    &mut iseq,
                                    out,
                                );
                                redirect(&replica, &mut pending, key);
                            }
                            None => {
                                let _ = reply.send(OpResult::SchemaError(
                                    "malformed request frame".into(),
                                ));
                            }
                        }
                    }
                }
                Ev::Peer { from, msg } => {
                    let out = replica.handle(from, msg);
                    process(&mut replica, &mut pending, &mut iseq, out);
                }
                Ev::Tick => {
                    let out = replica.tick();
                    process(&mut replica, &mut pending, &mut iseq, out);
                }
                Ev::Probe(ptx) => {
                    let _ = ptx.send((
                        replica.digest(),
                        replica.op_number(),
                        replica.committed(),
                    ));
                }
            }
        }
    });

    match ready_rx.recv() {
        Ok(Ok(())) => Ok(Node {
            tx: etx,
            client_seq: Arc::new(AtomicU64::new(1)),
            session_seq: Arc::new(AtomicU64::new(0)),
        }),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(io::Error::new(io::ErrorKind::Other, "engine failed to start")),
    }
}

fn handle_client_conn(mut s: TcpStream, node: Arc<Node>) {
    loop {
        let req = match read_frame(&mut s) {
            Ok(r) => r,
            Err(_) => break,
        };
        // A `0xFD` session frame carries a stable (client, req) so a
        // cross-node failover retry is exactly-once; route it through the
        // dedup-aware path. Anything else keeps the legacy behaviour.
        let res = match kessel_client::parse_session_frame(&req) {
            Some((client, rq, op)) => node.submit_as(client, rq, op),
            None => node.apply_raw(req),
        };
        if write_frame(&mut s, &res.encode()).is_err() {
            break;
        }
    }
}

/// Serve the ordinary client protocol (`kessel-client`, incl. `sql()`) for
/// this cluster node, one thread per connection. Connect clients to the
/// primary: replies are emitted there (failover client-reply routing is a
/// documented follow-up).
pub fn serve_clients(listener: TcpListener, node: Arc<Node>) {
    for stream in listener.incoming().flatten() {
        let n = node.clone();
        std::thread::spawn(move || handle_client_conn(stream, n));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_catalog::{encode_type_def, Field, FieldKind};
    use kessel_proto::ObjectId;
    use std::time::Instant;

    fn wait_converged(nodes: &[Node], want_commit: u64) -> bool {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(10) {
            let p: Vec<_> = nodes.iter().map(|n| n.probe()).collect();
            let d0 = p[0].0;
            if p.iter().all(|x| x.0 == d0 && x.2 >= want_commit) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }

    #[test]
    fn three_nodes_replicate_over_real_tcp() {
        let n = 3;
        // Bind all peer listeners first so every node knows every address.
        let listeners: Vec<TcpListener> = (0..n)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addrs: Vec<SocketAddr> =
            listeners.iter().map(|l| l.local_addr().unwrap()).collect();

        let mut nodes = Vec::new();
        let mut dirs = Vec::new();
        for (i, l) in listeners.into_iter().enumerate() {
            let dir = std::env::temp_dir()
                .join(format!("kesseldb-cluster-{}-{i}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            dirs.push(dir.clone());
            nodes.push(spawn_node(i, l, addrs.clone(), dir).unwrap());
        }
        // Let dial/accept links establish.
        std::thread::sleep(Duration::from_millis(200));

        // Client talks to the primary (node 0 is primary in view 0).
        let primary = &nodes[0];
        assert_eq!(
            primary.submit(Op::CreateType {
                def: encode_type_def(
                    "acct",
                    &[Field {
                        field_id: 0,
                        name: "bal".into(),
                        kind: FieldKind::U64,
                        nullable: false,
                    }],
                ),
            }),
            OpResult::TypeCreated(1)
        );
        let id = ObjectId::from_u128(42);
        assert_eq!(
            primary.submit(Op::Create { type_id: 1, id, record: vec![7, 7, 7] }),
            OpResult::Ok
        );
        // Linearized read through consensus returns the committed value.
        assert_eq!(
            primary.submit(Op::GetById { type_id: 1, id }),
            OpResult::Got(vec![7, 7, 7])
        );
        // An atomic txn over the real cluster.
        assert_eq!(
            primary.submit(Op::Txn {
                ops: vec![
                    Op::Create { type_id: 1, id: ObjectId::from_u128(2), record: vec![1] },
                    Op::Create { type_id: 1, id: ObjectId::from_u128(3), record: vec![2] },
                ],
            }),
            OpResult::Ok
        );

        // Replication proof: every node converges to the SAME state digest
        // over the socket transport (>=4 ops committed everywhere).
        assert!(
            wait_converged(&nodes, 4),
            "nodes did not converge over real TCP: {:?}",
            nodes.iter().map(|n| n.probe()).collect::<Vec<_>>()
        );
        for d in &dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    }

    #[test]
    fn sql_over_cluster_full_crud_and_rmw() {
        use kessel_client::Client;

        let n = 3;
        let listeners: Vec<TcpListener> = (0..n)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addrs: Vec<SocketAddr> =
            listeners.iter().map(|l| l.local_addr().unwrap()).collect();

        let mut dirs = Vec::new();
        let mut listeners = listeners.into_iter();
        // node 0 = primary (view 0); keep it separate as an Arc so the
        // client front can share it without moving it out of a Vec.
        let dir0 = std::env::temp_dir()
            .join(format!("kesseldb-sqlcluster-{}-0", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir0);
        dirs.push(dir0.clone());
        let node0 = Arc::new(
            spawn_node(0, listeners.next().unwrap(), addrs.clone(), dir0).unwrap(),
        );
        let mut followers = Vec::new();
        for i in 1..n {
            let dir = std::env::temp_dir()
                .join(format!("kesseldb-sqlcluster-{}-{i}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            dirs.push(dir.clone());
            followers.push(
                spawn_node(i, listeners.next().unwrap(), addrs.clone(), dir).unwrap(),
            );
        }
        std::thread::sleep(Duration::from_millis(200));

        // Expose the primary's client protocol on a real TCP port.
        let cl = TcpListener::bind("127.0.0.1:0").unwrap();
        let caddr = cl.local_addr().unwrap();
        {
            let n0 = node0.clone();
            std::thread::spawn(move || serve_clients(cl, n0));
        }

        let mut c = Client::connect(caddr).unwrap();
        assert!(matches!(
            c.sql("CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)")
                .unwrap(),
            OpResult::TypeCreated(1)
        ));
        assert_eq!(
            c.sql("INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)")
                .unwrap(),
            OpResult::Ok
        );
        assert_eq!(
            c.sql("INSERT INTO acct ID 2 (owner, bal) VALUES (100, 999)")
                .unwrap(),
            OpResult::Ok
        );
        match c.sql("SELECT SUM(bal) FROM acct WHERE owner = 100").unwrap() {
            OpResult::Got(b) => {
                assert_eq!(i128::from_le_bytes(b.try_into().unwrap()), 1049)
            }
            o => panic!("unexpected {o:?}"),
        }
        // SQL UPDATE = read-modify-write across consensus (two replicated
        // rounds: linearized GetById, then the patched Update).
        assert_eq!(c.sql("UPDATE acct ID 1 SET bal = 500").unwrap(), OpResult::Ok);
        match c.sql("SELECT SUM(bal) FROM acct WHERE owner = 100").unwrap() {
            OpResult::Got(b) => {
                assert_eq!(i128::from_le_bytes(b.try_into().unwrap()), 1499)
            }
            o => panic!("unexpected {o:?}"),
        }
        assert_eq!(
            c.sql("UPDATE acct ID 999 SET bal = 1").unwrap(),
            OpResult::NotFound
        );
        match c.sql("SELECT * FROM acct ID 2").unwrap() {
            OpResult::Got(rec) => assert!(!rec.is_empty()),
            o => panic!("unexpected {o:?}"),
        }

        // All three nodes converged to one digest over the wire.
        let probe0 = node0.probe();
        assert!(
            wait_converged(&followers, probe0.2),
            "followers did not converge after SQL-over-cluster"
        );
        for (k, f) in followers.iter().enumerate() {
            assert_eq!(
                probe0.0,
                f.probe().0,
                "primary/follower {} digests diverged",
                k + 1
            );
        }

        for d in &dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    }

    #[test]
    fn session_retry_is_exactly_once() {
        let n = 3;
        let listeners: Vec<TcpListener> = (0..n)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addrs: Vec<SocketAddr> =
            listeners.iter().map(|l| l.local_addr().unwrap()).collect();
        let mut nodes = Vec::new();
        let mut dirs = Vec::new();
        for (i, l) in listeners.into_iter().enumerate() {
            let dir = std::env::temp_dir()
                .join(format!("kesseldb-sess-{}-{i}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            dirs.push(dir.clone());
            nodes.push(spawn_node(i, l, addrs.clone(), dir).unwrap());
        }
        std::thread::sleep(Duration::from_millis(200));
        let primary = &nodes[0];

        // Setup schema via the bare path (irrelevant to the dedup proof).
        assert_eq!(
            primary.submit(Op::CreateType {
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

        let s = primary.session();
        let id = ObjectId::from_u128(7);
        // req 1: create the row -> Ok.
        assert_eq!(
            s.submit_with_req(Op::Create { type_id: 1, id, record: vec![1] }, 1),
            OpResult::Ok
        );
        std::thread::sleep(Duration::from_millis(60));
        let digest_after_create = primary.probe().0;

        // RETRY of the *same* (client, req=1): a client that lost the reply
        // and resent. Must return the CACHED result (Ok) and NOT re-apply.
        assert_eq!(
            s.submit_with_req(Op::Create { type_id: 1, id, record: vec![1] }, 1),
            OpResult::Ok,
            "retried (client,req) must return the cached reply, not re-execute"
        );
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(
            primary.probe().0,
            digest_after_create,
            "state digest changed on a duplicate request — op applied twice"
        );

        // Proof the row really exists exactly once: a *different* client
        // creating the same id now collides.
        assert_eq!(
            primary.submit(Op::Create { type_id: 1, id, record: vec![9] }),
            OpResult::Exists
        );

        // A genuinely new request number on the same session still works.
        let id2 = ObjectId::from_u128(8);
        assert_eq!(
            s.submit_with_req(Op::Create { type_id: 1, id: id2, record: vec![2] }, 2),
            OpResult::Ok
        );

        assert!(
            wait_converged(&nodes, 1),
            "nodes did not converge after session ops"
        );
        for d in &dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    }

    #[test]
    fn failover_retry_against_follower_returns_cached_reply() {
        let n = 3;
        let listeners: Vec<TcpListener> = (0..n)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addrs: Vec<SocketAddr> =
            listeners.iter().map(|l| l.local_addr().unwrap()).collect();
        let mut nodes = Vec::new();
        let mut dirs = Vec::new();
        for (i, l) in listeners.into_iter().enumerate() {
            let dir = std::env::temp_dir()
                .join(format!("kesseldb-fail-{}-{i}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            dirs.push(dir.clone());
            nodes.push(spawn_node(i, l, addrs.clone(), dir).unwrap());
        }
        std::thread::sleep(Duration::from_millis(200));

        assert_eq!(
            nodes[0].submit(Op::CreateType {
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

        // Client talks to the primary under a stable session.
        let s = nodes[0].session();
        let cid = s.client_id();
        let id = ObjectId::from_u128(7);
        assert_eq!(
            s.submit_with_req(Op::Create { type_id: 1, id, record: vec![1] }, 1),
            OpResult::Ok
        );
        // Wait until every node (incl. the follower) has applied it.
        assert!(wait_converged(&nodes, 1), "did not converge before failover");
        let follower_digest = nodes[1].probe().0;

        // Primary "fails": the client reconnects to a FOLLOWER and retries
        // the exact same (client, req=1). The follower answers from its
        // replicated client table — original reply, no re-execution.
        assert_eq!(
            nodes[1].submit_as(cid, 1, Op::Create { type_id: 1, id, record: vec![1] }),
            OpResult::Ok,
            "follower must serve the cached reply for a committed (client,req)"
        );
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(
            nodes[1].probe().0,
            follower_digest,
            "follower re-applied a retried request — not exactly-once on failover"
        );
        // Sanity: a fresh client creating the same id collides (exists once).
        assert_eq!(
            nodes[0].submit(Op::Create { type_id: 1, id, record: vec![9] }),
            OpResult::Exists
        );

        for d in &dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    }

    fn poll_converged(nodes: &[Arc<Node>], want_commit: u64) -> bool {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(10) {
            let p: Vec<_> = nodes.iter().map(|nd| nd.probe()).collect();
            let d0 = p[0].0;
            if p.iter().all(|x| x.0 == d0 && x.2 >= want_commit) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }

    #[test]
    fn cluster_client_finds_primary_and_is_exactly_once() {
        use kessel_client::{session_frame, ClusterClient};

        let n = 3;
        let peer_ls: Vec<TcpListener> = (0..n)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addrs: Vec<SocketAddr> =
            peer_ls.iter().map(|l| l.local_addr().unwrap()).collect();

        let mut dirs = Vec::new();
        let mut client_addrs = Vec::new();
        let mut arc_nodes: Vec<Arc<Node>> = Vec::new();
        for (i, l) in peer_ls.into_iter().enumerate() {
            let dir = std::env::temp_dir()
                .join(format!("kesseldb-cc-{}-{i}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            dirs.push(dir.clone());
            let node = Arc::new(spawn_node(i, l, addrs.clone(), dir).unwrap());
            let cl = TcpListener::bind("127.0.0.1:0").unwrap();
            client_addrs.push(cl.local_addr().unwrap().to_string());
            let nn = node.clone();
            std::thread::spawn(move || serve_clients(cl, nn));
            arc_nodes.push(node);
        }
        std::thread::sleep(Duration::from_millis(200));

        // Address list with the PRIMARY (node 0) LAST, so the client must
        // rotate past two followers (which answer Unavailable) to find it.
        let ordered = vec![
            client_addrs[1].clone(),
            client_addrs[2].clone(),
            client_addrs[0].clone(),
        ];
        let mut c = ClusterClient::new(ordered);

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
            OpResult::TypeCreated(1),
            "ClusterClient must rotate past followers and reach the primary"
        );
        let id = ObjectId::from_u128(5);
        // req 2 — keep this the LATEST request: a VSR client has one
        // outstanding request at a time and only ever retries its latest,
        // so the client table (which keeps the last (req,result)) dedupes
        // exactly that case.
        assert_eq!(
            c.call(&Op::Create { type_id: 1, id, record: vec![3] }).unwrap(),
            OpResult::Ok
        );

        // Exactly-once across the wire: replay the *identical* committed
        // session frame straight to a follower's client port; it must
        // return the cached reply and NOT re-apply (digest stable).
        assert!(
            poll_converged(&arc_nodes, 2),
            "cluster did not converge before the replay check"
        );
        let foll_digest = arc_nodes[1].probe().0;
        // req 1 = CreateType, req 2 = the Create above. Replay req 2.
        let frame = session_frame(
            c.client_id(),
            2,
            &Op::Create { type_id: 1, id, record: vec![3] },
        );
        let mut raw = TcpStream::connect(&client_addrs[1]).unwrap();
        write_frame(&mut raw, &frame).unwrap();
        let resp = read_frame(&mut raw).unwrap();
        assert_eq!(
            OpResult::decode(&resp),
            Some(OpResult::Ok),
            "follower must serve the cached reply for a replayed session frame"
        );
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(
            arc_nodes[1].probe().0,
            foll_digest,
            "replayed session frame re-applied — not exactly-once"
        );

        // Client still works for a fresh request after all the rotation.
        assert_eq!(
            c.call(&Op::GetById { type_id: 1, id }).unwrap(),
            OpResult::Got(vec![3])
        );

        for d in &dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    }

    #[test]
    fn cluster_sql_cache_correct_across_ddl() {
        use kessel_client::Client;

        let n = 3;
        let listeners: Vec<TcpListener> = (0..n)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addrs: Vec<SocketAddr> =
            listeners.iter().map(|l| l.local_addr().unwrap()).collect();
        let mut dirs = Vec::new();
        let mut listeners = listeners.into_iter();
        let dir0 = std::env::temp_dir()
            .join(format!("kesseldb-sqlc51-{}-0", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir0);
        dirs.push(dir0.clone());
        let node0 = Arc::new(
            spawn_node(0, listeners.next().unwrap(), addrs.clone(), dir0).unwrap(),
        );
        for i in 1..n {
            let dir = std::env::temp_dir()
                .join(format!("kesseldb-sqlc51-{}-{i}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            dirs.push(dir.clone());
            spawn_node(i, listeners.next().unwrap(), addrs.clone(), dir).unwrap();
        }
        std::thread::sleep(Duration::from_millis(200));
        let cl = TcpListener::bind("127.0.0.1:0").unwrap();
        let caddr = cl.local_addr().unwrap();
        {
            let n0 = node0.clone();
            std::thread::spawn(move || serve_clients(cl, n0));
        }

        let mut c = Client::connect(caddr).unwrap();
        assert!(matches!(
            c.sql("CREATE TABLE a (v U64 NOT NULL)").unwrap(),
            OpResult::TypeCreated(1)
        ));
        assert_eq!(c.sql("INSERT INTO a ID 1 (v) VALUES (7)").unwrap(), OpResult::Ok);

        // Compile + cache, then a cache hit at the same epoch — identical.
        let r1 = c.sql("SELECT * FROM a ID 1").unwrap();
        let r2 = c.sql("SELECT * FROM a ID 1").unwrap();
        assert!(matches!(r1, OpResult::Got(_)));
        assert_eq!(r1, r2, "cluster cache hit must be identical");

        // A DDL changes the catalog → epoch bumps → cached plans for the
        // *new* statements are recompiled against the new schema, and the
        // old one is still correct (not a stale-epoch entry).
        assert!(matches!(
            c.sql("CREATE TABLE b (w U64 NOT NULL)").unwrap(),
            OpResult::TypeCreated(2)
        ));
        assert_eq!(c.sql("SELECT * FROM a ID 1").unwrap(), r1, "post-DDL still correct");
        assert_eq!(c.sql("INSERT INTO b ID 1 (w) VALUES (9)").unwrap(), OpResult::Ok);
        assert!(matches!(
            c.sql("SELECT * FROM b ID 1").unwrap(),
            OpResult::Got(_)
        ));
        // RMW path through the cluster cache still works.
        assert_eq!(c.sql("UPDATE a ID 1 SET v = 50").unwrap(), OpResult::Ok);
        assert_eq!(c.sql("UPDATE a ID 1 SET v = 50").unwrap(), OpResult::Ok);

        for d in &dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    }
}
