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

use kessel_io::DirVfs;
use kessel_proto::wire::{read_frame, write_frame};
use kessel_proto::{ClientId, Op, OpResult};
use kessel_sm::StateMachine;
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
    Peer { from: usize, msg: Msg },
    Tick,
    Probe(SyncSender<(u32, u64, u64)>),
}

/// A running node. Holds the engine channel; `submit` linearizes an op
/// through VSR and blocks for the committed reply.
pub struct Node {
    tx: Sender<Ev>,
    client_seq: Arc<AtomicU64>,
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
        // (client, req) -> waiting caller, for routing the committed reply.
        let mut pending: HashMap<(ClientId, u64), SyncSender<OpResult>> = HashMap::new();

        let dispatch =
            |replica: &mut Replica<DirVfs>,
             pending: &mut HashMap<(ClientId, u64), SyncSender<OpResult>>,
             out: kessel_vsr::Out| {
                for (to, msg) in out.msgs {
                    if to == self_idx {
                        continue;
                    }
                    if let Some(w) = writers.get(&to) {
                        let _ = w.send(wire::encode(&msg));
                    }
                }
                for (client, req, res) in out.replies {
                    if let Some(s) = pending.remove(&(client, req)) {
                        let _ = s.send(res);
                    }
                }
                let _ = replica;
            };

        while let Ok(ev) = erx.recv() {
            match ev {
                Ev::Client { client, req, op, reply } => {
                    pending.insert((client, req), reply);
                    let out =
                        replica.handle(self_idx, Msg::Request { client, req, op });
                    dispatch(&mut replica, &mut pending, out);
                }
                Ev::Peer { from, msg } => {
                    let out = replica.handle(from, msg);
                    dispatch(&mut replica, &mut pending, out);
                }
                Ev::Tick => {
                    let out = replica.tick();
                    dispatch(&mut replica, &mut pending, out);
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
        Ok(Ok(())) => Ok(Node { tx: etx, client_seq: Arc::new(AtomicU64::new(1)) }),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(io::Error::new(io::ErrorKind::Other, "engine failed to start")),
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
}
