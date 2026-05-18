//! Multi-shard router (SP78) — the substrate for cross-shard
//! transactions.
//!
//! A KesselDB *shard group* is an independent VSR cluster (one
//! [`crate::cluster`] deployment). A deployment can now run **K** of
//! them; this router sits in front and sends each request to the shard
//! that owns its key, using the deterministic rendezvous map
//! ([`kessel_shard::ShardMap`]) that has existed as groundwork since
//! M4 and is finally wired into a runtime here.
//!
//! Scope of this slice (honest, incremental): the router speaks the
//! ordinary client wire at the **operation** level —
//!
//! - point ops (`Create`/`Update`/`Delete`/`GetById`) → the one owning
//!   shard;
//! - schema/DDL ops → **broadcast** to every shard (shards must keep
//!   identical catalogs so per-shard execution stays deterministic);
//! - `Op::Txn` whose members all map to one shard → that shard
//!   (per-shard atomic, exactly as a single cluster already is);
//! - `Op::Txn` spanning shards → detected and **cleanly rejected**
//!   (a deterministic cross-shard commit is the next slice — this slice
//!   makes multi-shard correct, not silently wrong);
//! - scatter-gather reads / SQL text are explicitly **not** routed yet
//!   (a clear error, not a wrong answer) — a later slice.
//!
//! Router-level client exactly-once across shards is also a later
//! slice; each per-shard hop is already exactly-once via
//! [`kessel_client::ClusterClient`].

use kessel_client::ClusterClient;
use kessel_proto::wire::{read_frame, write_frame};
use kessel_proto::{Op, OpResult};
use kessel_shard::ShardMap;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

/// Where a request must go.
#[derive(Debug, PartialEq, Eq)]
enum Route {
    /// Exactly one shard owns this key.
    One(usize),
    /// Schema/DDL — every shard, identically.
    All,
    /// A transaction spanning these shards (sorted, len ≥ 2).
    Cross(Vec<usize>),
    /// Not routable by this slice (clear error, never a wrong answer).
    Unsupported(&'static str),
}

/// Front for K shard groups. Cheap to clone the address lists; the
/// per-connection shard clients are created lazily by [`serve_router`].
pub struct Router {
    shard_addrs: Vec<Vec<String>>,
    map: ShardMap,
    token: Option<Vec<u8>>,
}

/// The 20-byte storage key for a row (`type_id` LE ++ `object_id`),
/// identical to `kessel_storage::make_key` — the unit the rendezvous
/// map hashes.
fn row_key(type_id: u32, id: &[u8; 16]) -> Vec<u8> {
    let mut k = Vec::with_capacity(20);
    k.extend_from_slice(&type_id.to_le_bytes());
    k.extend_from_slice(id);
    k
}

impl Router {
    /// `shard_addrs[i]` = the client-address list of shard group `i`
    /// (any order; the per-shard `ClusterClient` finds its primary).
    pub fn new(shard_addrs: Vec<Vec<String>>) -> Self {
        let k = shard_addrs.len().max(1) as u32;
        Router { shard_addrs, map: ShardMap::new(k), token: None }
    }

    /// Authenticate every shard hop with this shared-secret token.
    pub fn with_token(mut self, token: Vec<u8>) -> Self {
        self.token = Some(token);
        self
    }

    pub fn shards(&self) -> usize {
        self.shard_addrs.len()
    }

    fn shard_of(&self, type_id: u32, id: &[u8; 16]) -> usize {
        self.map.shard_of(&row_key(type_id, id)) as usize
    }

    /// Pure routing decision for one op (the heart of the slice;
    /// unit-tested directly).
    fn route(&self, op: &Op) -> Route {
        match op {
            Op::Create { type_id, id, .. }
            | Op::Update { type_id, id, .. }
            | Op::Delete { type_id, id }
            | Op::GetById { type_id, id } => {
                Route::One(self.shard_of(*type_id, &id.0))
            }
            // Schema is global: every shard must apply identical DDL in
            // the same order so per-shard execution stays deterministic.
            Op::CreateType { .. }
            | Op::AlterTypeAddField { .. }
            | Op::CreateIndex { .. }
            | Op::AddUnique { .. }
            | Op::AddForeignKey { .. }
            | Op::AddCheck { .. }
            | Op::AddTrigger { .. }
            | Op::AddOrderedIndex { .. }
            | Op::AddCompositeIndex { .. }
            | Op::DropType { .. }
            | Op::DropIndex { .. }
            | Op::DropField { .. }
            | Op::RenameField { .. }
            | Op::AddBalanceGuard { .. } => Route::All,
            // Catalog is identical on every shard — answer from one.
            Op::Describe { .. } => Route::One(0),
            Op::Txn { ops } => {
                let mut set = std::collections::BTreeSet::new();
                for o in ops {
                    match o {
                        Op::Create { type_id, id, .. }
                        | Op::Update { type_id, id, .. }
                        | Op::Delete { type_id, id }
                        | Op::GetById { type_id, id } => {
                            set.insert(self.shard_of(*type_id, &id.0));
                        }
                        _ => {
                            return Route::Unsupported(
                                "Txn with a non-point op is not routable \
                                 (point ops only across shards)",
                            )
                        }
                    }
                }
                match set.len() {
                    0 | 1 => Route::One(set.into_iter().next().unwrap_or(0)),
                    _ => Route::Cross(set.into_iter().collect()),
                }
            }
            _ => Route::Unsupported(
                "router (multi-shard, this slice) handles point ops, DDL, \
                 and single/rejected-cross transactions; scatter-gather \
                 reads and SQL text are a later slice",
            ),
        }
    }
}

/// One client connection: lazily-built per-shard `ClusterClient`s, the
/// ordinary client wire (bare `Op::encode()` or `0xFD` session frames).
struct Conn<'a> {
    router: &'a Router,
    clients: Vec<Option<ClusterClient>>,
}

impl<'a> Conn<'a> {
    fn client(&mut self, i: usize) -> &mut ClusterClient {
        if self.clients[i].is_none() {
            let mut c = ClusterClient::new(self.router.shard_addrs[i].clone());
            if let Some(t) = &self.router.token {
                c = c.with_token(t.clone());
            }
            self.clients[i] = Some(c);
        }
        self.clients[i].as_mut().unwrap()
    }

    fn forward(&mut self, op: &Op) -> OpResult {
        match self.router.route(op) {
            Route::One(i) => self
                .client(i)
                .call(op)
                .unwrap_or_else(|e| OpResult::SchemaError(format!("shard {i}: {e}"))),
            Route::All => {
                // Broadcast in shard order; every shard starts identical
                // and gets the identical DDL stream, so results agree.
                let mut first: Option<OpResult> = None;
                for i in 0..self.router.shards() {
                    let r = self.client(i).call(op).unwrap_or_else(|e| {
                        OpResult::SchemaError(format!("shard {i}: {e}"))
                    });
                    match &first {
                        None => first = Some(r),
                        Some(f) if *f != r => {
                            return OpResult::SchemaError(format!(
                                "shard {i} DDL result diverged: {f:?} vs {r:?}"
                            ))
                        }
                        _ => {}
                    }
                }
                first.unwrap_or(OpResult::Ok)
            }
            Route::Cross(set) => OpResult::SchemaError(format!(
                "cross-shard transaction spans shards {set:?}; deterministic \
                 cross-shard commit lands in a later slice (this slice keeps \
                 multi-shard correct, not silently wrong)"
            )),
            Route::Unsupported(why) => OpResult::SchemaError(why.into()),
        }
    }
}

/// Serve the ordinary client protocol in front of K shard groups, one
/// thread per connection.
pub fn serve_router(listener: TcpListener, router: Arc<Router>) {
    for stream in listener.incoming().flatten() {
        let _ = stream.set_nodelay(true);
        let r = router.clone();
        std::thread::spawn(move || handle(stream, r));
    }
}

fn handle(mut s: TcpStream, router: Arc<Router>) {
    let mut conn = Conn {
        router: &router,
        clients: (0..router.shards()).map(|_| None).collect(),
    };
    loop {
        let req = match read_frame(&mut s) {
            Ok(r) => r,
            Err(_) => break,
        };
        // `0xFD` session frame → its op (router-level exactly-once is a
        // later slice; the per-shard hop is already exactly-once).
        let op = match kessel_client::parse_session_frame(&req) {
            Some((_, _, op)) => Some(op),
            None => Op::decode(&req),
        };
        let res = match op {
            Some(o) => conn.forward(&o),
            None => OpResult::SchemaError(
                "router: expected an Op frame (SQL text is a later slice)"
                    .into(),
            ),
        };
        if write_frame(&mut s, &res.encode()).is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{serve_clients, spawn_node};
    use kessel_catalog::{encode_type_def, Field, FieldKind};
    use kessel_client::Client;
    use kessel_proto::ObjectId;
    use std::net::SocketAddr;
    use std::time::Duration;

    // A shard group = an independent 3-node VSR cluster (the proven
    // configuration; a 1-node "cluster" never reaches a commit quorum).
    // Returns the three client addresses.
    fn spawn_shard(tag: &str) -> Vec<String> {
        let n = 3;
        let peers: Vec<TcpListener> = (0..n)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let paddrs: Vec<SocketAddr> =
            peers.iter().map(|l| l.local_addr().unwrap()).collect();
        let mut caddrs = Vec::new();
        for (i, pl) in peers.into_iter().enumerate() {
            let dir = std::env::temp_dir().join(format!(
                "kesseldb-router-{}-{tag}-{i}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let node =
                Arc::new(spawn_node(i, pl, paddrs.clone(), dir).unwrap());
            let cl = TcpListener::bind("127.0.0.1:0").unwrap();
            caddrs.push(cl.local_addr().unwrap().to_string());
            std::thread::spawn(move || serve_clients(cl, node));
        }
        caddrs
    }

    #[test]
    fn router_routes_points_broadcasts_ddl_and_rejects_cross_shard() {
        let s0 = spawn_shard("a");
        let s1 = spawn_shard("b");
        let router = Arc::new(Router::new(vec![s0.clone(), s1.clone()]));
        let rl = TcpListener::bind("127.0.0.1:0").unwrap();
        let raddr = rl.local_addr().unwrap();
        {
            let r = router.clone();
            std::thread::spawn(move || serve_router(rl, r));
        }
        // Let 6 nodes (2 groups × 3) establish peer links + elect.
        std::thread::sleep(Duration::from_millis(1200));

        let mut c = Client::connect(raddr).unwrap();
        // DDL broadcast: identical TypeCreated on every shard ⇒ one reply.
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

        // Find two ids that route to different shards.
        let m = ShardMap::new(2);
        let mut ida = None;
        let mut idb = None;
        for v in 1u128..500 {
            let id = ObjectId::from_u128(v);
            let sh = m.shard_of(&row_key(1, &id.0)) as usize;
            if sh == 0 && ida.is_none() {
                ida = Some(v);
            }
            if sh == 1 && idb.is_none() {
                idb = Some(v);
            }
            if ida.is_some() && idb.is_some() {
                break;
            }
        }
        let (ida, idb) = (ida.unwrap(), idb.unwrap());

        // Each point write lands on exactly its owning shard.
        assert_eq!(
            c.call(&Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(ida),
                record: vec![1, 0, 0, 0, 0, 0, 0, 0],
            })
            .unwrap(),
            OpResult::Ok
        );
        assert_eq!(
            c.call(&Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(idb),
                record: vec![2, 0, 0, 0, 0, 0, 0, 0],
            })
            .unwrap(),
            OpResult::Ok
        );
        // Verify placement by talking to each shard directly.
        let mut d0 = ClusterClient::new(s0);
        let mut d1 = ClusterClient::new(s1);
        assert!(matches!(
            d0.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(ida) }).unwrap(),
            OpResult::Got(_)
        ));
        assert_eq!(
            d0.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(idb) }).unwrap(),
            OpResult::NotFound,
            "idb must NOT be on shard 0"
        );
        assert!(matches!(
            d1.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(idb) }).unwrap(),
            OpResult::Got(_)
        ));
        assert_eq!(
            d1.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(ida) }).unwrap(),
            OpResult::NotFound
        );

        // Read routed through the router returns the owning shard's row.
        assert!(matches!(
            c.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(ida) }).unwrap(),
            OpResult::Got(_)
        ));

        // Single-shard txn: two FRESH ids (disjoint from ida/idb and the
        // earlier writes) that both route to the same shard.
        let same: Vec<u128> = (5000u128..20000)
            .filter(|v| {
                *v != ida
                    && *v != idb
                    && m.shard_of(&row_key(1, &ObjectId::from_u128(*v).0)) == 0
            })
            .take(2)
            .collect();
        assert_eq!(
            c.call(&Op::Txn {
                ops: vec![
                    Op::Create { type_id: 1, id: ObjectId::from_u128(same[0]), record: vec![3,0,0,0,0,0,0,0] },
                    Op::Create { type_id: 1, id: ObjectId::from_u128(same[1]), record: vec![4,0,0,0,0,0,0,0] },
                ],
            })
            .unwrap(),
            OpResult::Ok
        );

        // Cross-shard txn is rejected cleanly with NO partial effect.
        let r = c
            .call(&Op::Txn {
                ops: vec![
                    Op::Create { type_id: 1, id: ObjectId::from_u128(ida), record: vec![9,0,0,0,0,0,0,0] },
                    Op::Create { type_id: 1, id: ObjectId::from_u128(idb), record: vec![9,0,0,0,0,0,0,0] },
                ],
            })
            .unwrap();
        assert!(
            matches!(r, OpResult::SchemaError(ref m) if m.contains("cross-shard")),
            "cross-shard txn must be cleanly rejected, got {r:?}"
        );
        // ida still has its ORIGINAL value (1), not the txn's 9 — no
        // partial write leaked.
        assert_eq!(
            d0.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(ida) }).unwrap(),
            OpResult::Got(vec![1, 0, 0, 0, 0, 0, 0, 0])
        );
    }

    #[test]
    fn route_decisions_are_correct() {
        let r = Router::new(vec![vec!["a".into()], vec!["b".into()]]);
        assert!(matches!(
            r.route(&Op::CreateType { def: vec![] }),
            Route::All
        ));
        assert!(matches!(
            r.route(&Op::Describe { type_id: 1 }),
            Route::One(0)
        ));
        let one = r.route(&Op::GetById { type_id: 1, id: ObjectId::from_u128(7) });
        assert!(matches!(one, Route::One(_)));
        // A txn split across shards is Cross; on one shard is One.
        let m = ShardMap::new(2);
        let a = (1u128..999)
            .find(|v| m.shard_of(&row_key(1, &ObjectId::from_u128(*v).0)) == 0)
            .unwrap();
        let b = (1u128..999)
            .find(|v| m.shard_of(&row_key(1, &ObjectId::from_u128(*v).0)) == 1)
            .unwrap();
        assert!(matches!(
            r.route(&Op::Txn {
                ops: vec![
                    Op::Delete { type_id: 1, id: ObjectId::from_u128(a) },
                    Op::Delete { type_id: 1, id: ObjectId::from_u128(b) },
                ]
            }),
            Route::Cross(_)
        ));
        assert!(matches!(
            r.route(&Op::Txn {
                ops: vec![Op::Delete { type_id: 1, id: ObjectId::from_u128(a) }]
            }),
            Route::One(_)
        ));
        assert!(matches!(
            r.route(&Op::Select { type_id: 1, program: vec![], limit: 0 }),
            Route::Unsupported(_)
        ));
    }
}
