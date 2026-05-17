//! kesseldb-server: a runnable single-node TCP server.
//!
//! The deterministic core (`kessel-sm`) lives on ONE owning thread and never
//! moves; connection threads talk to it over a channel. So apply is serial
//! (matching the single-threaded-core design) and the engine never needs to
//! be `Send`. The server is just the real-I/O edge; the engine stays pure.
//! VSR-over-sockets (multi-node networking) is still deferred and documented.

#![forbid(unsafe_code)]

use kessel_io::DirVfs;
use kessel_proto::wire::{read_frame, write_frame};
use kessel_proto::{Op, OpResult};
use kessel_sm::StateMachine;
use std::io;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender};

/// One request to the engine thread: an op and a one-shot reply channel.
type EngineMsg = (Op, SyncSender<OpResult>);

/// Handle used by connection threads to submit ops to the single engine.
#[derive(Clone)]
pub struct EngineHandle {
    tx: Sender<EngineMsg>,
}

impl EngineHandle {
    pub fn apply(&self, op: Op) -> OpResult {
        let (rtx, rrx) = sync_channel(1);
        if self.tx.send((op, rtx)).is_err() {
            return OpResult::SchemaError("engine stopped".into());
        }
        rrx.recv()
            .unwrap_or_else(|_| OpResult::SchemaError("engine dropped reply".into()))
    }
}

/// Spawn the owning engine thread (it opens the data dir itself, since
/// `StateMachine<DirVfs>` is not `Send`). Blocks until the engine is ready
/// or returns the open error.
pub fn spawn_engine(data_dir: impl AsRef<Path>) -> io::Result<EngineHandle> {
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
        while let Ok((op, reply)) = rx.recv() {
            let r = sm.apply(n, op);
            n += 1;
            let _ = reply.send(r);
        }
    });
    match ready_rx.recv() {
        Ok(Ok(())) => Ok(EngineHandle { tx }),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(io::Error::new(io::ErrorKind::Other, "engine failed to start")),
    }
}

fn handle_conn(mut stream: TcpStream, engine: EngineHandle) {
    loop {
        let req = match read_frame(&mut stream) {
            Ok(r) => r,
            Err(_) => break,
        };
        let result = match Op::decode(&req) {
            Some(op) => engine.apply(op),
            None => OpResult::SchemaError("malformed request frame".into()),
        };
        if write_frame(&mut stream, &result.encode()).is_err() {
            break;
        }
    }
}

/// Serve forever on `listener`, one thread per connection.
pub fn serve(listener: TcpListener, engine: EngineHandle) {
    for stream in listener.incoming().flatten() {
        let e = engine.clone();
        std::thread::spawn(move || handle_conn(stream, e));
    }
}

/// Open the data dir and serve on `addr` (blocking).
pub fn run(addr: impl ToSocketAddrs, data_dir: impl AsRef<Path>) -> io::Result<()> {
    let engine = spawn_engine(data_dir)?;
    let listener = TcpListener::bind(addr)?;
    serve(listener, engine);
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
}
