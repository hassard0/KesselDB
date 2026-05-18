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
    /// Router-side op: handled entirely in the router, not forwarded
    /// to any shard.
    Refresh,
    /// Not routable by this slice (clear error, never a wrong answer).
    Unsupported(&'static str),
}

/// Front for K shard groups. Cheap to clone the address lists; the
/// per-connection shard clients are created lazily by [`serve_router`].
pub struct Router {
    shard_addrs: Vec<Vec<String>>,
    /// The global sequencer group's client addresses (SP79). When set,
    /// a cross-shard `Op::Txn` is sequenced and deterministically
    /// applied to every shard in seq order (Calvin-style); when empty
    /// it is cleanly rejected (slice-1 behaviour).
    seq_addrs: Vec<String>,
    map: ShardMap,
    token: Option<Vec<u8>>,
    /// Serializes cross-shard commits so global seqs are *driven* to
    /// every shard strictly in order (each shard's in-order cursor then
    /// trivially accepts them). Async pull-drive is a later slice.
    xs: std::sync::Mutex<()>,
    /// Per-process salt + counter for bare-Op clients' dedup keys
    /// (session-frame clients get true exactly-once via (client,req);
    /// bare-Op clients get a unique key per call ⇒ at-least-once, never
    /// a FALSE dedup — documented).
    salt: u64,
    nonce: std::sync::atomic::AtomicU64,
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
        Router {
            shard_addrs,
            seq_addrs: Vec::new(),
            map: ShardMap::new(k),
            token: None,
            xs: std::sync::Mutex::new(()),
            salt: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0)
                ^ (std::process::id() as u64),
            nonce: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Enable deterministic cross-shard transactions by giving the
    /// router the sequencer group's client addresses (SP80).
    pub fn with_sequencer(mut self, seq_addrs: Vec<String>) -> Self {
        self.seq_addrs = seq_addrs;
        self
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
            // External-source DDL is catalog state: every shard must
            // apply identical Create/Drop in the same order, exactly
            // like CreateType above.
            Op::CreateExternalSource { .. }
            | Op::DropExternalSource { .. } => Route::All,
            // REFRESH is router-side: fetch then submit captured rows.
            Op::RefreshExternalSource { .. } => Route::Refresh,
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
    seq: Option<ClusterClient>,
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

    fn seq_client(&mut self) -> &mut ClusterClient {
        if self.seq.is_none() {
            let mut c = ClusterClient::new(self.router.seq_addrs.clone());
            if let Some(t) = &self.router.token {
                c = c.with_token(t.clone());
            }
            self.seq = Some(c);
        }
        self.seq.as_mut().unwrap()
    }

    /// Encode per-shard slices as the durable descriptor:
    /// `[u32 k]` then `k × ([u32 len][Op::Txn encode])`.
    fn encode_desc(slices: &[Vec<Op>]) -> Vec<u8> {
        let mut d = (slices.len() as u32).to_le_bytes().to_vec();
        for sl in slices {
            let enc = Op::Txn { ops: sl.clone() }.encode();
            d.extend_from_slice(&(enc.len() as u32).to_le_bytes());
            d.extend_from_slice(&enc);
        }
        d
    }

    fn decode_desc(d: &[u8]) -> Option<Vec<Vec<Op>>> {
        let k = u32::from_le_bytes(d.get(0..4)?.try_into().ok()?) as usize;
        let mut p = 4usize;
        let mut slices = Vec::with_capacity(k);
        for _ in 0..k {
            let l =
                u32::from_le_bytes(d.get(p..p + 4)?.try_into().ok()?) as usize;
            p += 4;
            let chunk = d.get(p..p + l)?;
            p += l;
            match Op::decode(chunk)? {
                Op::Txn { ops } => slices.push(ops),
                _ => return None,
            }
        }
        Some(slices)
    }

    /// Deterministic two-phase drive of one global `seq` (SP81). Phase 1
    /// (decide): every participating shard dry-runs its slice and
    /// returns a STABLE verdict; the global decision is the AND — a pure
    /// function of durable per-shard state, so any router (incl. a
    /// restarted one in `recover`) computes the same result with no
    /// coordinator. Phase 2 (commit): every shard advances its cursor
    /// for `seq`, applying the slice iff the decision was commit, else
    /// an atomic deterministic skip. Idempotent end to end.
    fn drive_seq(
        &mut self,
        seq: u64,
        slices: &[Vec<Op>],
    ) -> Result<bool, String> {
        let k = slices.len();
        let mut decision = true;
        for i in 0..k {
            if slices[i].is_empty() {
                continue; // non-participant: no vote
            }
            match self.client(i).call(&Op::XshardDecide {
                seq,
                ops: slices[i].clone(),
            }) {
                Ok(OpResult::Got(v)) if v.len() == 1 => {
                    if v[0] == 0 {
                        decision = false;
                    }
                }
                Ok(o) => return Err(format!("decide shard {i}: {o:?}")),
                Err(e) => return Err(format!("decide shard {i}: {e}")),
            }
        }
        for i in 0..k {
            match self.client(i).call(&Op::XshardCommit {
                seq,
                ops: slices[i].clone(),
                commit: decision,
            }) {
                Ok(OpResult::Ok) => {}
                Ok(o) => return Err(format!("commit shard {i}: {o:?}")),
                Err(e) => return Err(format!("commit shard {i}: {e}")),
            }
        }
        Ok(decision)
    }

    /// Cross-shard commit with deterministic abort agreement and
    /// exactly-once. `dedup` makes a client/router retry append the
    /// SAME descriptor to the SAME seq (no double-apply).
    fn commit_cross_shard(
        &mut self,
        members: Vec<Op>,
        dedup: Vec<u8>,
    ) -> OpResult {
        let k = self.router.shards();
        let mut slices: Vec<Vec<Op>> = (0..k).map(|_| Vec::new()).collect();
        for o in members {
            match &o {
                Op::Create { type_id, id, .. }
                | Op::Update { type_id, id, .. }
                | Op::Delete { type_id, id } => {
                    let s = self.router.shard_of(*type_id, &id.0);
                    slices[s].push(o);
                }
                _ => {
                    return OpResult::SchemaError(
                        "cross-shard txn members must be Create/Update/\
                         Delete"
                            .into(),
                    )
                }
            }
        }
        let desc = Self::encode_desc(&slices);
        let _guard = self.router.xs.lock().unwrap();
        // Exactly-once durable ordering: a retry returns the same seq.
        let seq = match self.seq_client().call(&Op::SeqAppendOnce {
            key: dedup,
            payload: desc,
        }) {
            Ok(OpResult::Got(b)) if b.len() == 8 => {
                u64::from_le_bytes(b.try_into().unwrap())
            }
            Ok(o) => {
                return OpResult::SchemaError(format!(
                    "sequencer returned unexpected {o:?}"
                ))
            }
            Err(e) => return OpResult::SchemaError(format!("sequencer: {e}")),
        };
        match self.drive_seq(seq, &slices) {
            Ok(true) => OpResult::Ok,
            Ok(false) => OpResult::Constraint(
                "cross-shard transaction aborted: a participant slice \
                 would fail (atomic — no shard applied it)"
                    .into(),
            ),
            Err(e) => OpResult::SchemaError(e),
        }
    }

    /// Re-drive the entire ordered cross-shard log idempotently — used
    /// after a router restart so a transaction durably appended but not
    /// fully driven is completed (decide is verdict-stable, commit is
    /// cursor-idempotent, so this never double-applies or diverges).
    fn recover(&mut self) -> Result<usize, String> {
        let log = match self
            .seq_client()
            .call(&Op::SeqRead { from: 1, limit: 0 })
        {
            Ok(OpResult::Got(b)) => b,
            Ok(o) => return Err(format!("seqread: {o:?}")),
            Err(e) => return Err(format!("seqread: {e}")),
        };
        let _guard = self.router.xs.lock().unwrap();
        let mut p = 0usize;
        let mut n = 0usize;
        while p + 12 <= log.len() {
            let seq = u64::from_le_bytes(log[p..p + 8].try_into().unwrap());
            let l = u32::from_le_bytes(log[p + 8..p + 12].try_into().unwrap())
                as usize;
            p += 12;
            let desc = &log[p..p + l];
            p += l;
            let slices = Self::decode_desc(desc)
                .ok_or_else(|| format!("bad descriptor at seq {seq}"))?;
            self.drive_seq(seq, &slices)?;
            n += 1;
        }
        Ok(n)
    }

    fn forward(&mut self, op: &Op, dedup: Vec<u8>) -> OpResult {
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
            Route::Cross(set) => {
                if self.router.seq_addrs.is_empty() {
                    return OpResult::SchemaError(format!(
                        "cross-shard transaction spans shards {set:?}; no \
                         sequencer configured (run with_sequencer)"
                    ));
                }
                match op {
                    Op::Txn { ops } => {
                        self.commit_cross_shard(ops.clone(), dedup)
                    }
                    _ => OpResult::SchemaError(
                        "cross-shard route on a non-Txn op".into(),
                    ),
                }
            }
            Route::Refresh => {
                #[cfg(feature = "external-sources")]
                {
                    self.do_refresh(op, dedup)
                }
                #[cfg(not(feature = "external-sources"))]
                {
                    let _ = dedup;
                    let what = match op {
                        Op::RefreshExternalSource { name } => name.as_str(),
                        _ => "<unknown>",
                    };
                    OpResult::SchemaError(format!(
                        "REFRESH `{what}`: server not built with \
                         --features external-sources"
                    ))
                }
            }
            Route::Unsupported(why) => OpResult::SchemaError(why.into()),
        }
    }
}

/// Router-side `REFRESH <name>` (EXT slice 1, behind the
/// `external-sources` feature). The fetch happens **once, here**; only
/// the captured rows re-enter the replicated log, as one atomic
/// `Op::Txn` of upserts driven through the EXISTING `forward` path
/// (so single-shard / cross-shard routing AND the exactly-once `dedup`
/// key are reused unchanged). A failed/partial fetch, or any codec/id
/// error before the Txn is submitted, mutates NOTHING.
#[cfg(feature = "external-sources")]
impl<'a> Conn<'a> {
    fn do_refresh(&mut self, op: &Op, dedup: Vec<u8>) -> OpResult {
        use kessel_catalog::{Catalog, ExternalAuth};
        use kessel_fetch::{
            fetch_rows, Auth, ColumnMap, Format, DEFAULT_MAX_BODY,
        };

        // 1. Resolve the source name.
        let name = match op {
            Op::RefreshExternalSource { name } => name.clone(),
            _ => {
                return OpResult::SchemaError(
                    "do_refresh: not a RefreshExternalSource op".into(),
                )
            }
        };

        // 2. Read the FULL catalog. The state machine persists the whole
        //    `Catalog` (incl. the `external` recipe trailer) under the
        //    single well-known storage key `make_key(0, [0;16])`
        //    (kessel_sm::catalog_key). `Op::GetById` is a generic,
        //    side-effect-free storage read with no type-existence guard,
        //    so reading type_id 0 / id 0 returns exactly that encoded
        //    blob. The catalog is identical on every shard (DDL is
        //    broadcast), so — like `Op::Describe` (`Route::One(0)`) — we
        //    answer from shard 0.
        let cat_blob = match self.client(0).call(&Op::GetById {
            type_id: 0,
            id: kessel_proto::ObjectId([0u8; 16]),
        }) {
            Ok(OpResult::Got(b)) => b,
            Ok(OpResult::NotFound) => {
                return OpResult::SchemaError(
                    "REFRESH: catalog is empty".into(),
                )
            }
            Ok(o) => {
                return OpResult::SchemaError(format!(
                    "REFRESH: unexpected catalog read result {o:?}"
                ))
            }
            Err(e) => {
                return OpResult::SchemaError(format!(
                    "REFRESH: catalog read failed: {e}"
                ))
            }
        };
        let cat = match Catalog::decode(&cat_blob) {
            Some(c) => c,
            None => {
                return OpResult::SchemaError(
                    "REFRESH: catalog decode failed".into(),
                )
            }
        };
        let ot = match cat.types.iter().find(|t| t.name == name) {
            Some(t) => t.clone(),
            None => return OpResult::NotFound,
        };
        let recipe = match cat
            .external
            .iter()
            .find(|e| e.type_id == ot.type_id)
        {
            Some(r) => r.clone(),
            None => {
                return OpResult::SchemaError(format!(
                    "REFRESH `{name}`: not an external source"
                ))
            }
        };

        // 3. Resolve auth from THIS process's env (a value, never put in
        //    an op or a log line; the recipe only persisted a reference).
        let auth = match &recipe.auth {
            ExternalAuth::None => Auth::None,
            ExternalAuth::BearerEnv(var) => match std::env::var(var) {
                Ok(v) => Auth::Bearer(v),
                Err(_) => {
                    return OpResult::SchemaError(format!(
                        "REFRESH `{name}`: auth env `{var}` not set"
                    ))
                }
            },
            ExternalAuth::HeaderEnv { header, env } => {
                match std::env::var(env) {
                    Ok(v) => Auth::Header {
                        name: header.clone(),
                        value: v,
                    },
                    Err(_) => {
                        return OpResult::SchemaError(format!(
                            "REFRESH `{name}`: auth env `{env}` not set"
                        ))
                    }
                }
            }
        };

        // 4. Build the column map (recipe.mapping joined with the type's
        //    fields by field_id, in mapping order) and fetch.
        let mut cols: Vec<ColumnMap> = Vec::with_capacity(recipe.mapping.len());
        // Parallel: the field for each mapped column, same order as the
        // fetched per-column bytes.
        let mut col_fields: Vec<&kessel_catalog::Field> =
            Vec::with_capacity(recipe.mapping.len());
        for (fid, source) in &recipe.mapping {
            let field = match ot.fields.iter().find(|f| f.field_id == *fid) {
                Some(f) => f,
                None => {
                    return OpResult::SchemaError(format!(
                        "REFRESH `{name}`: mapping references unknown \
                         field_id {fid}"
                    ))
                }
            };
            cols.push(ColumnMap {
                name: field.name.clone(),
                kind: field.kind,
                source: source.clone(),
            });
            col_fields.push(field);
        }
        let format = match recipe.format {
            0 => Format::Json,
            1 => Format::Csv,
            n => {
                return OpResult::SchemaError(format!(
                    "REFRESH `{name}`: unknown format code {n}"
                ))
            }
        };
        // The KEY column's INDEX within the fetched per-column vec (which
        // is in `recipe.mapping` order).
        let key_idx = match recipe
            .mapping
            .iter()
            .position(|(fid, _)| *fid == recipe.key_field_id)
        {
            Some(i) => i,
            None => {
                return OpResult::SchemaError(format!(
                    "REFRESH `{name}`: KEY field_id {} is not mapped",
                    recipe.key_field_id
                ))
            }
        };

        let rows = match fetch_rows(
            &recipe.url,
            &auth,
            format,
            &cols,
            DEFAULT_MAX_BODY,
        ) {
            Ok(r) => r,
            // Fetch/parse/type/auth/too-large — mutate NOTHING.
            Err(e) => {
                return OpResult::SchemaError(format!("refresh: {e}"))
            }
        };

        // 5. Build the codec record + deterministic ObjectId per row.
        //    Codec path: `kessel_codec::value_from_raw(kind, raw)` turns
        //    each column's raw fixed-width LE bytes into a `Value`
        //    (per FieldKind), then `kessel_codec::encode(&ot, &values)`
        //    assembles the record — the existing public codec API; no
        //    helper needed. The columns come back in `recipe.mapping`
        //    order; `ot.fields` is the canonical field order, so we
        //    place each mapped value at its field's index.
        let mut to_create: Vec<(kessel_proto::ObjectId, Vec<u8>)> = Vec::new();
        let mut to_upsert: Vec<(kessel_proto::ObjectId, Vec<u8>)> = Vec::new();
        for row in &rows {
            if row.len() != cols.len() {
                return OpResult::SchemaError(format!(
                    "refresh: row arity {} != mapped columns {}",
                    row.len(),
                    cols.len()
                ));
            }
            // Deterministic id from the KEY column's raw bytes.
            let key_raw = &row[key_idx];
            let mut pre: Vec<u8> = Vec::new();
            pre.extend_from_slice(b"kessel-ext-id\0");
            pre.extend_from_slice(&ot.type_id.to_le_bytes());
            pre.extend_from_slice(key_raw);
            let digest = kessel_crypto::sha256(&pre);
            let mut id = [0u8; 16];
            id.copy_from_slice(&digest[..16]);
            let oid = kessel_proto::ObjectId(id);

            // Values parallel to `ot.fields`. Mapped fields get the
            // fetched bytes; any unmapped field is NULL (it must then be
            // nullable, else `encode` rejects it — correct, surfaced
            // before any mutation).
            let mut values: Vec<kessel_codec::Value> =
                vec![kessel_codec::Value::Null; ot.fields.len()];
            for (ci, field) in col_fields.iter().enumerate() {
                let idx = ot
                    .fields
                    .iter()
                    .position(|f| f.field_id == field.field_id)
                    .expect("mapped field is in the type");
                values[idx] =
                    kessel_codec::value_from_raw(field.kind, &row[ci]);
            }
            let record = match kessel_codec::encode(&ot, &values) {
                Ok(r) => r,
                Err(e) => {
                    return OpResult::SchemaError(format!(
                        "refresh: record encode failed: {e:?}"
                    ))
                }
            };

            // 6. Create-vs-Update via a side-effect-free point existence
            //    check through the EXISTING route path.
            let exists = match self.forward(
                &Op::GetById {
                    type_id: ot.type_id,
                    id: oid,
                },
                dedup_probe(&dedup, &id),
            ) {
                OpResult::Got(_) => true,
                OpResult::NotFound => false,
                other => {
                    return OpResult::SchemaError(format!(
                        "refresh: existence probe failed: {other:?}"
                    ))
                }
            };
            if exists {
                to_upsert.push((oid, record));
            } else {
                to_create.push((oid, record));
            }
        }

        // Assemble ONE atomic Op::Txn (creates then updates) and submit
        // it through the EXISTING replicated path with the SAME `dedup`
        // (this is exactly why Task 10 threaded `dedup` — exactly-once is
        // preserved end to end; do NOT drop it).
        let mut txn_ops: Vec<Op> = Vec::with_capacity(rows.len());
        for (oid, record) in to_create {
            txn_ops.push(Op::Create {
                type_id: ot.type_id,
                id: oid,
                record,
            });
        }
        for (oid, record) in to_upsert {
            txn_ops.push(Op::Update {
                type_id: ot.type_id,
                id: oid,
                record,
            });
        }
        if txn_ops.is_empty() {
            // Nothing upstream: a successful no-op refresh.
            return OpResult::Ok;
        }
        match self.forward(&Op::Txn { ops: txn_ops }, dedup) {
            OpResult::Ok => OpResult::Ok,
            other => other,
        }
    }
}

/// Derive a stable, distinct dedup key for a per-row read probe so it
/// can never collide with the refresh's write `dedup`. Reads are
/// side-effect-free, so this only matters for cross-shard read framing.
#[cfg(feature = "external-sources")]
fn dedup_probe(base: &[u8], id: &[u8; 16]) -> Vec<u8> {
    let mut d = Vec::with_capacity(base.len() + 17);
    d.push(b'p');
    d.extend_from_slice(base);
    d.extend_from_slice(id);
    d
}

/// Serve the ordinary client protocol in front of K shard groups, one
/// thread per connection.
/// Re-drive the entire ordered cross-shard log idempotently (call
/// after a router restart). Returns how many descriptors were
/// re-driven. Safe to call any time: decide is verdict-stable and
/// commit is cursor-idempotent, so this never double-applies.
pub fn recover(router: &Arc<Router>) -> Result<usize, String> {
    let mut conn = Conn {
        router,
        clients: (0..router.shards()).map(|_| None).collect(),
        seq: None,
    };
    conn.recover()
}

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
        seq: None,
    };
    loop {
        let req = match read_frame(&mut s) {
            Ok(r) => r,
            Err(_) => break,
        };
        // `0xFD` session frame → its op (router-level exactly-once is a
        // later slice; the per-shard hop is already exactly-once).
        // Dedup key for exactly-once cross-shard: a session frame's
        // stable (client,req) gives true exactly-once; a bare-Op frame
        // gets a unique per-call key (at-least-once; never a false
        // dedup) — documented, consistent with the rest of the system.
        let (op, dedup) = match kessel_client::parse_session_frame(&req) {
            Some((c, r, op)) => {
                let mut d = vec![b's'];
                d.extend_from_slice(&c.to_le_bytes());
                d.extend_from_slice(&r.to_le_bytes());
                (Some(op), d)
            }
            None => {
                let n = conn
                    .router
                    .nonce
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut d = vec![b'n'];
                d.extend_from_slice(&conn.router.salt.to_le_bytes());
                d.extend_from_slice(&n.to_le_bytes());
                (Op::decode(&req), d)
            }
        };
        let res = match op {
            Some(o) => conn.forward(&o, dedup),
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
        assert_eq!(
            r.route(&Op::RefreshExternalSource { name: "s".into() }),
            Route::Refresh
        );
        assert_eq!(
            r.route(&Op::CreateExternalSource {
                name: "s".into(), type_def: vec![], url: String::new(),
                format: 0, key_field_id: 1, auth_kind: 0,
                auth_a: String::new(), auth_b: String::new(), mapping: vec![],
            }),
            Route::All
        );
        assert_eq!(
            r.route(&Op::DropExternalSource { name: "s".into() }),
            Route::All
        );
    }

    /// SP80 (slice 3): with a sequencer configured, a cross-shard
    /// `Op::Txn` is deterministically committed — durably ordered, then
    /// applied to every owning shard. Atomic placement verified by
    /// talking to each shard directly.
    #[test]
    fn cross_shard_txn_commits_atomically_via_sequencer() {
        let s0 = spawn_shard("xa");
        let s1 = spawn_shard("xb");
        let seq = spawn_shard("xseq");
        let router = Arc::new(
            Router::new(vec![s0.clone(), s1.clone()]).with_sequencer(seq),
        );
        let rl = TcpListener::bind("127.0.0.1:0").unwrap();
        let raddr = rl.local_addr().unwrap();
        {
            let r = router.clone();
            std::thread::spawn(move || serve_router(rl, r));
        }
        std::thread::sleep(Duration::from_millis(1600));

        let mut c = Client::connect(raddr).unwrap();
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

        let m = ShardMap::new(2);
        let pick = |want: usize, skip: &[u128]| -> u128 {
            (1u128..5000)
                .find(|v| {
                    !skip.contains(v)
                        && m.shard_of(&row_key(1, &ObjectId::from_u128(*v).0))
                            as usize
                            == want
                })
                .unwrap()
        };
        let a1 = pick(0, &[]);
        let b1 = pick(1, &[]);

        // Cross-shard txn: one row per shard, atomic.
        assert_eq!(
            c.call(&Op::Txn {
                ops: vec![
                    Op::Create { type_id: 1, id: ObjectId::from_u128(a1), record: vec![1,0,0,0,0,0,0,0] },
                    Op::Create { type_id: 1, id: ObjectId::from_u128(b1), record: vec![2,0,0,0,0,0,0,0] },
                ],
            })
            .unwrap(),
            OpResult::Ok
        );

        // Each row landed on exactly its owning shard.
        let mut d0 = ClusterClient::new(s0.clone());
        let mut d1 = ClusterClient::new(s1.clone());
        assert!(matches!(
            d0.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(a1) }).unwrap(),
            OpResult::Got(_)
        ));
        assert!(matches!(
            d1.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(b1) }).unwrap(),
            OpResult::Got(_)
        ));
        assert_eq!(
            d1.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(a1) }).unwrap(),
            OpResult::NotFound
        );
        assert_eq!(
            d0.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(b1) }).unwrap(),
            OpResult::NotFound
        );

        // A second cross-shard txn (next global seq) also commits.
        let a2 = pick(0, &[a1]);
        let b2 = pick(1, &[b1]);
        assert_eq!(
            c.call(&Op::Txn {
                ops: vec![
                    Op::Create { type_id: 1, id: ObjectId::from_u128(a2), record: vec![3,0,0,0,0,0,0,0] },
                    Op::Create { type_id: 1, id: ObjectId::from_u128(b2), record: vec![4,0,0,0,0,0,0,0] },
                ],
            })
            .unwrap(),
            OpResult::Ok
        );
        assert!(matches!(
            d0.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(a2) }).unwrap(),
            OpResult::Got(_)
        ));
        assert!(matches!(
            d1.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(b2) }).unwrap(),
            OpResult::Got(_)
        ));
    }

    /// SP81 (slice 4): a cross-shard txn whose one slice would fail
    /// aborts on EVERY shard (deterministic agreement); a replayed
    /// session-framed cross-shard txn is applied exactly once; and a
    /// full log re-drive (router restart) is idempotent.
    #[test]
    fn cross_shard_aborts_atomically_is_exactly_once_and_recovers() {
        use kessel_client::session_frame;
        use kessel_proto::wire::{read_frame, write_frame};
        use std::net::TcpStream;

        let s0 = spawn_shard("ra");
        let s1 = spawn_shard("rb");
        let seq = spawn_shard("rseq");
        let router = Arc::new(
            Router::new(vec![s0.clone(), s1.clone()]).with_sequencer(seq),
        );
        let rl = TcpListener::bind("127.0.0.1:0").unwrap();
        let raddr = rl.local_addr().unwrap();
        {
            let r = router.clone();
            std::thread::spawn(move || serve_router(rl, r));
        }
        std::thread::sleep(Duration::from_millis(1600));

        let mut c = Client::connect(raddr).unwrap();
        assert_eq!(
            c.call(&Op::CreateType {
                def: encode_type_def(
                    "t",
                    &[Field { field_id: 0, name: "v".into(), kind: FieldKind::U64, nullable: false }],
                ),
            })
            .unwrap(),
            OpResult::TypeCreated(1)
        );
        let m = ShardMap::new(2);
        let pick = |want: usize, skip: &[u128]| -> u128 {
            (1u128..6000)
                .find(|v| {
                    !skip.contains(v)
                        && m.shard_of(&row_key(1, &ObjectId::from_u128(*v).0))
                            as usize
                            == want
                })
                .unwrap()
        };
        let rec = |n: u8| vec![n, 0, 0, 0, 0, 0, 0, 0];
        let mut d0 = ClusterClient::new(s0.clone());
        let mut d1 = ClusterClient::new(s1.clone());

        // --- atomic abort: dup on shard 0, fresh on shard 1 ---
        let a_dup = pick(0, &[]);
        let b_fresh = pick(1, &[]);
        assert_eq!(
            c.call(&Op::Create { type_id: 1, id: ObjectId::from_u128(a_dup), record: rec(1) }).unwrap(),
            OpResult::Ok
        );
        let r = c
            .call(&Op::Txn {
                ops: vec![
                    Op::Create { type_id: 1, id: ObjectId::from_u128(a_dup), record: rec(9) }, // dup ⇒ fail on shard 0
                    Op::Create { type_id: 1, id: ObjectId::from_u128(b_fresh), record: rec(2) },
                ],
            })
            .unwrap();
        assert!(
            matches!(r, OpResult::Constraint(_)),
            "cross-shard txn with a failing slice must abort, got {r:?}"
        );
        assert_eq!(
            d1.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(b_fresh) }).unwrap(),
            OpResult::NotFound,
            "atomic: the other shard's slice must NOT have applied"
        );
        assert_eq!(
            d0.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(a_dup) }).unwrap(),
            OpResult::Got(rec(1)),
            "the pre-existing row is unchanged"
        );

        // --- exactly-once: replay the SAME session (client,req) ---
        let a3 = pick(0, &[a_dup]);
        let b3 = pick(1, &[b_fresh]);
        let txn = Op::Txn {
            ops: vec![
                Op::Create { type_id: 1, id: ObjectId::from_u128(a3), record: rec(3) },
                Op::Create { type_id: 1, id: ObjectId::from_u128(b3), record: rec(4) },
            ],
        };
        let frame = session_frame(0xABCD, 1, &txn);
        let mut raw = TcpStream::connect(raddr).unwrap();
        for _ in 0..2 {
            write_frame(&mut raw, &frame).unwrap();
            let resp = read_frame(&mut raw).unwrap();
            assert_eq!(
                OpResult::decode(&resp).unwrap(),
                OpResult::Ok,
                "both deliveries of the same (client,req) reply Ok"
            );
        }
        // Applied exactly once: a fresh create of a3 now says Exists
        // (it exists), and there is no second/duplicate effect.
        assert!(matches!(
            d0.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(a3) }).unwrap(),
            OpResult::Got(_)
        ));
        assert!(matches!(
            d1.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(b3) }).unwrap(),
            OpResult::Got(_)
        ));

        // --- recovery: a full ordered re-drive is idempotent ---
        let n = super::recover(&router).expect("recover");
        assert!(n >= 2, "recover re-drove the ordered log ({n} entries)");
        // State is exactly as before recovery.
        assert!(matches!(
            d0.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(a3) }).unwrap(),
            OpResult::Got(_)
        ));
        assert_eq!(
            d1.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(b_fresh) }).unwrap(),
            OpResult::NotFound,
            "the aborted txn stays aborted after recovery (stable verdict)"
        );
    }

    /// SP82 (slice 5): many concurrent cross-shard txns over real
    /// sockets all commit atomically (the `xs` lock serialises the
    /// global order), every row lands on its owning shard, and a
    /// post-hoc full recovery re-drive changes nothing.
    #[test]
    fn concurrent_cross_shard_txns_are_atomic_over_sockets() {
        let s0 = spawn_shard("ca");
        let s1 = spawn_shard("cb");
        let seq = spawn_shard("cseq");
        let router = Arc::new(
            Router::new(vec![s0.clone(), s1.clone()]).with_sequencer(seq),
        );
        let rl = TcpListener::bind("127.0.0.1:0").unwrap();
        let raddr = rl.local_addr().unwrap();
        {
            let r = router.clone();
            std::thread::spawn(move || serve_router(rl, r));
        }
        std::thread::sleep(Duration::from_millis(1600));

        Client::connect(raddr)
            .unwrap()
            .call(&Op::CreateType {
                def: encode_type_def(
                    "t",
                    &[Field { field_id: 0, name: "v".into(), kind: FieldKind::U64, nullable: false }],
                ),
            })
            .unwrap();

        let m = ShardMap::new(2);
        let pick = |want: usize, nth: usize| -> u128 {
            (1u128..50000)
                .filter(|v| {
                    m.shard_of(&row_key(1, &ObjectId::from_u128(*v).0)) as usize
                        == want
                })
                .nth(nth)
                .unwrap()
        };
        let n = 8usize;
        let handles: Vec<_> = (0..n)
            .map(|t| {
                let a = pick(0, t);
                let b = pick(1, t);
                std::thread::spawn(move || {
                    let mut c = Client::connect(raddr).unwrap();
                    let r = c
                        .call(&Op::Txn {
                            ops: vec![
                                Op::Create { type_id: 1, id: ObjectId::from_u128(a), record: vec![1,0,0,0,0,0,0,0] },
                                Op::Create { type_id: 1, id: ObjectId::from_u128(b), record: vec![2,0,0,0,0,0,0,0] },
                            ],
                        })
                        .unwrap();
                    assert_eq!(r, OpResult::Ok, "concurrent cross-shard txn {t}");
                    (a, b)
                })
            })
            .collect();
        let pairs: Vec<(u128, u128)> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();

        let mut d0 = ClusterClient::new(s0);
        let mut d1 = ClusterClient::new(s1);
        for (a, b) in &pairs {
            assert!(matches!(
                d0.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(*a) }).unwrap(),
                OpResult::Got(_)
            ));
            assert!(matches!(
                d1.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(*b) }).unwrap(),
                OpResult::Got(_)
            ));
        }
        // A full recovery re-drive after concurrent commits is a no-op.
        assert!(super::recover(&router).expect("recover") >= n);
        for (a, b) in &pairs {
            assert!(matches!(
                d0.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(*a) }).unwrap(),
                OpResult::Got(_)
            ));
            assert!(matches!(
                d1.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(*b) }).unwrap(),
                OpResult::Got(_)
            ));
        }
    }
}
