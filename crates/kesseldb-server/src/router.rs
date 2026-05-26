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

use crate::scatter_scan::{
    merge_scan_results, scatter_scan_fanout, ScatterKind, ShardCaller,
    DEFAULT_PER_SHARD_TIMEOUT,
};
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
    /// SP-A (SP155): scatter the op to every shard, merge the per-
    /// shard results per `ScatterKind`. The router does the work;
    /// the wire stays unchanged (clients keep sending `Op::Select`
    /// / `Op::SelectSorted` / etc.). See `crate::scatter_scan` and
    /// the SP155 design spec §3.2.
    Scatter(ScatterKind),
    /// Not routable by this slice (clear error, never a wrong answer).
    Unsupported(&'static str),
}

/// SP-A T2: `ClusterClient` IS the per-shard caller for the scatter
/// path — `scatter_scan_fanout` owns one of these per shard. The
/// `ShardCaller` trait wraps `ClusterClient::call`'s `io::Result` into
/// the `Result<OpResult, String>` shape the scatter machinery needs;
/// transport errors become the shard's `OpResult::Unavailable` slot
/// (hard-fail per SP155 §6).
impl ShardCaller for ClusterClient {
    fn call(&mut self, op: &Op) -> Result<OpResult, String> {
        ClusterClient::call(self, op).map_err(|e| e.to_string())
    }
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
            // SP-A (SP155 §3.2): the four scan ops scatter to every
            // shard. The merge strategy is derived from the op's shape:
            // `SelectSorted` ⇒ k-way heap (Sorted); the rest ⇒ shard-
            // id-ordered concat (Unordered).
            //
            // The `sort_kind` / `sort_offset` / `sort_width` for the
            // Sorted variant are catalog-derived; we discover them at
            // dispatch time inside `Conn::scatter_read` (the router
            // has access to the catalog via the per-shard `Describe`
            // call). At `route()` time we only have the `Op`, so the
            // discriminator carries the op's raw `sort_field` and the
            // call site resolves the layout (cached per shard 0 per
            // OQ5). `Conn::scatter_read` does the resolution; `route()`
            // returns a marker that captures everything the merge
            // needs that's already known from the `Op`.
            //
            // Implementation choice (T2): `Route::Scatter(ScatterKind)`
            // carries only the catalog-INDEPENDENT bits (limit, desc,
            // offset). The catalog-dependent extras (`sort_kind`,
            // `sort_offset`, `sort_width`) are filled in by
            // `Conn::scatter_read` after a catalog lookup. We stash
            // the `sort_field` field-id inside the marker via a
            // placeholder `ScatterKind::Sorted` with width=0 — the
            // call site recognizes this shape and re-resolves. (An
            // alternative would be a separate `Route::ScatterSorted`
            // variant; chose to keep `Route` narrow.)
            Op::Select { limit, .. }
            | Op::QueryRows { limit, .. }
            | Op::SelectFields { limit, .. } => {
                Route::Scatter(ScatterKind::Unordered { limit: *limit })
            }
            Op::SelectSorted { sort_field, desc, offset, limit, .. } => {
                // Width=0 is the sentinel meaning "resolve at the
                // call site against the catalog". The catalog-aware
                // resolver lives in `Conn::scatter_read`. We pack
                // sort_field into `sort_offset` so the resolver knows
                // which field to look up (the marker has nowhere else
                // to put it; this is an intra-module convention, NOT
                // a stable wire shape — `ScatterKind` is purely an
                // internal router type per SP155 §4.1).
                Route::Scatter(ScatterKind::Sorted {
                    sort_kind: kessel_catalog::FieldKind::U8, // placeholder
                    sort_offset: *sort_field as u32,
                    sort_width: 0, // sentinel ⇒ resolve at call site
                    desc: *desc,
                    offset: *offset,
                    limit: *limit,
                })
            }
            _ => Route::Unsupported(
                "router (multi-shard, this slice) handles point ops, DDL, \
                 single/rejected-cross transactions, and scatter-gather \
                 reads for Select/QueryRows/SelectFields/SelectSorted; \
                 Aggregate/GroupAggregate (SP-B/SP-D), FindBy/FindByComposite \
                 (T11 follow-up), Join (non-goal), and SQL text (SP-E) are \
                 still later slices",
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

    /// SP-A (SP155 §3.1): driver for `Route::Scatter`. Builds a
    /// per-shard `ClusterClient` snapshot, fans out the SAME `op` to
    /// every shard in parallel via `scatter_scan_fanout`, then merges
    /// the per-shard `OpResult`s per `kind`.
    ///
    /// For `ScatterKind::Sorted` with `sort_width == 0` (the sentinel
    /// `route()` puts in to signal "resolve at the call site"), this
    /// resolves the sort field's `(kind, offset, width)` from shard
    /// 0's catalog via `Op::Describe` BEFORE fanning out — so the
    /// merger has the layout info per SP155 §3.5. Per SP155 OQ5: the
    /// per-shard catalogs are identical (DDL is broadcast), so shard
    /// 0 is the canonical source. Fast-fail if the type / sort field
    /// doesn't exist (errors surface as `SchemaError`).
    fn scatter_read(&mut self, op: &Op, kind: ScatterKind) -> OpResult {
        // 1. Resolve catalog-dependent merge parameters (Sorted only).
        let resolved_kind = match kind {
            ScatterKind::Unordered { .. } => kind,
            ScatterKind::Sorted {
                sort_offset,
                sort_width,
                desc,
                offset,
                limit,
                ..
            } if sort_width == 0 => {
                // `sort_offset` carries the field-id sentinel from
                // `route()`; resolve it against shard 0's catalog.
                let sort_field = sort_offset as u16;
                let type_id = match op {
                    Op::SelectSorted { type_id, .. } => *type_id,
                    _ => {
                        return OpResult::SchemaError(
                            "scatter: Sorted route on a non-SelectSorted op"
                                .into(),
                        )
                    }
                };
                let def_blob = match self
                    .client(0)
                    .call(&Op::Describe { type_id })
                {
                    Ok(OpResult::Got(b)) => b,
                    Ok(OpResult::NotFound) => {
                        return OpResult::SchemaError(format!(
                            "scatter: type {type_id} not found"
                        ))
                    }
                    Ok(o) => {
                        return OpResult::SchemaError(format!(
                            "scatter: describe shard 0 returned {o:?}"
                        ))
                    }
                    Err(e) => {
                        return OpResult::SchemaError(format!(
                            "scatter: describe shard 0: {e}"
                        ))
                    }
                };
                // Decode the type def (name + fields). The wire format
                // is `kessel_catalog::decode_type_def` — same shape
                // the SM's Op::Describe encodes from.
                let (_name, fields) =
                    match kessel_catalog::decode_type_def(&def_blob) {
                        Some(p) => p,
                        None => {
                            return OpResult::SchemaError(
                                "scatter: catalog describe blob decode failed"
                                    .into(),
                            )
                        }
                    };
                // Walk fields in declaration order to find the sort
                // field + its byte offset within the record. The
                // record layout starts with `HEADER_BYTES` then each
                // field at `kind.width()` increments — mirrors
                // `kessel_catalog::ObjectType::compute_layout()`. We
                // recompute it here from the parsed fields list since
                // we don't have a full `ObjectType` to call
                // `compute_layout()` on.
                let mut record_offset: usize =
                    kessel_catalog::HEADER_BYTES;
                let mut found: Option<(
                    kessel_catalog::FieldKind,
                    usize,
                    usize,
                )> = None;
                for f in &fields {
                    let w = f.kind.width() as usize;
                    if f.field_id == sort_field {
                        found = Some((f.kind, record_offset, w));
                        break;
                    }
                    record_offset += w;
                }
                let (sk, soff, sw) = match found {
                    Some(t) => t,
                    None => {
                        return OpResult::SchemaError(format!(
                            "scatter: sort field {sort_field} not in \
                             type {type_id}"
                        ))
                    }
                };
                ScatterKind::Sorted {
                    sort_kind: sk,
                    sort_offset: soff as u32,
                    sort_width: sw as u32,
                    desc,
                    offset,
                    limit,
                }
            }
            // Already-resolved Sorted (call sites that pre-resolve).
            other @ ScatterKind::Sorted { .. } => other,
        };
        // 2. Build per-shard `ClusterClient` snapshots. We must hand
        //    fresh, owned clients to the worker threads — the `Conn`'s
        //    cached `self.clients[i]` are exclusive references. Per-
        //    request clone is acceptable: TCP handshakes are lazy so
        //    each clone connects on first call (SP155 OQ10 — lazy
        //    pre-warming is V1 behavior).
        let k = self.router.shards();
        let mut shards: Vec<ClusterClient> = Vec::with_capacity(k);
        for i in 0..k {
            let mut c =
                ClusterClient::new(self.router.shard_addrs[i].clone());
            if let Some(t) = &self.router.token {
                c = c.with_token(t.clone());
            }
            shards.push(c);
        }
        // 3. Fan out + merge.
        let results = scatter_scan_fanout(
            shards,
            op,
            DEFAULT_PER_SHARD_TIMEOUT,
        );
        merge_scan_results(results, &resolved_kind)
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
            Route::Scatter(kind) => self.scatter_read(op, kind),
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
        use kessel_catalog::{Catalog, ExternalAuth, PaginationRecipe};
        use kessel_fetch::{
            fetch_rows, fetch_rows_paginated, Auth, ColumnMap, Format,
            Pagination, DEFAULT_MAX_BODY,
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

        // Object-store sources (`s3://` / `az://`) take a separate path
        // that signs the GET router-side and reuses the SAME post-fetch
        // tail. Dispatched BEFORE the env-Bearer/Header auth resolution so
        // an object-store recipe never touches the HTTP-auth code.
        let is_obj = recipe.url.starts_with("s3://")
            || recipe.url.starts_with("az://");
        if is_obj {
            #[cfg(feature = "external-sources-objstore")]
            {
                return self.do_refresh_objstore(&recipe, &ot, &name, dedup);
            }
            #[cfg(not(feature = "external-sources-objstore"))]
            {
                let _ = &dedup;
                return OpResult::SchemaError(format!(
                    "REFRESH `{name}`: object-store sources require the \
                     external-sources-objstore build feature"
                ));
            }
        }

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
            // OBJSTORE creds only make sense on an `s3://` / `az://`
            // recipe, which is already dispatched above. Reaching here
            // means a non-object URL was paired with OBJSTORE auth.
            ExternalAuth::ObjStoreEnv { .. } => {
                return OpResult::SchemaError(format!(
                    "REFRESH `{name}`: OBJSTORE credentials require an \
                     s3:// or az:// source URL"
                ))
            }
        };

        // 4. Build the column map (recipe.mapping joined with the type's
        //    fields by field_id, in mapping order) and fetch.
        let mut cols: Vec<ColumnMap> = Vec::with_capacity(recipe.mapping.len());
        // Parallel: the field and its index in `ot.fields` for each
        // mapped column, same order as the fetched per-column bytes.
        // Capturing the index here avoids a re-scan per row (FIX #3).
        let mut col_fields: Vec<(&kessel_catalog::Field, usize)> =
            Vec::with_capacity(recipe.mapping.len());
        for (fid, source) in &recipe.mapping {
            let (idx, field) = match ot
                .fields
                .iter()
                .enumerate()
                .find(|(_, f)| f.field_id == *fid)
            {
                Some(p) => p,
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
            col_fields.push((field, idx));
        }
        let format = match recipe.format {
            0 => Format::Json,
            1 => Format::Csv,
            2 => Format::Ndjson,
            3 => Format::Parquet,
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
        // `col_fields` / `key_idx` are still built here so the HTTP path's
        // pre-fetch validation early-returns at the EXACT original point
        // (byte-identical observable behavior). The post-fetch tail now
        // recomputes them verbatim inside `materialize_external_rows`
        // (shared with the object-store path).
        let _ = (&col_fields, &key_idx);

        // Single fetch step. With no PAGE clause this is exactly the
        // slice-1 one-shot `fetch_rows`; with a PAGE recipe it walks
        // pages via `fetch_rows_paginated` (itself all-or-nothing — on
        // Err nothing below runs, so NOTHING is submitted). Everything
        // after `rows` (id/codec/Txn/all-or-nothing) is unchanged.
        let rows = {
            let res = match &recipe.pagination {
                None => fetch_rows(
                    &recipe.url,
                    &auth,
                    format,
                    &cols,
                    DEFAULT_MAX_BODY,
                ),
                Some(pr) => {
                    let pg = match pr {
                        PaginationRecipe::NextUrlJson(p) => {
                            Pagination::NextUrlJson(p.clone())
                        }
                        PaginationRecipe::NextLink => {
                            Pagination::NextLink
                        }
                        PaginationRecipe::CursorJson {
                            path,
                            param,
                        } => Pagination::CursorJson {
                            path: path.clone(),
                            param: param.clone(),
                        },
                    };
                    fetch_rows_paginated(
                        &recipe.url,
                        &auth,
                        format,
                        &cols,
                        recipe.rows_path.as_deref(),
                        &pg,
                        DEFAULT_MAX_BODY,
                    )
                }
            };
            match res {
                Ok(r) => r,
                // Fetch/parse/type/auth/too-large/loop — mutate NOTHING.
                Err(e) => {
                    return OpResult::SchemaError(format!("refresh: {e}"))
                }
            }
        };

        // 5–6 + Txn submission: the post-fetch tail is a behavior-neutral
        // extraction (`materialize_external_rows`); the bytes submitted are
        // byte-identical to the previous inline tail (proven by the
        // external_source_oracle / external_source_tls_oracle staying green).
        self.materialize_external_rows(&recipe, &ot, &name, &cols, rows, dedup)
    }

    /// The post-fetch tail of `do_refresh`, extracted VERBATIM so the
    /// object-store path (`do_refresh_objstore`) can reuse it with
    /// byte-identical submission semantics. `key_idx` / `col_fields` are
    /// recomputed here from `recipe` + `ot` EXACTLY as the original inline
    /// step-4 logic did (same order, same error strings) so the captured
    /// rows produce the SAME deterministic `ObjectId`s, the SAME codec
    /// records, and the SAME single all-or-nothing `Op::Txn` through the
    /// existing `forward` path with the SAME `dedup`.
    fn materialize_external_rows(
        &mut self,
        recipe: &kessel_catalog::ExternalRecipe,
        ot: &kessel_catalog::ObjectType,
        name: &str,
        cols: &[kessel_fetch::ColumnMap],
        rows: Vec<Vec<Vec<u8>>>,
        dedup: Vec<u8>,
    ) -> OpResult {
        // Parallel: the field and its index in `ot.fields` for each
        // mapped column, same order as the fetched per-column bytes.
        // Capturing the index here avoids a re-scan per row (FIX #3).
        // Recomputed verbatim from the original step-4 build.
        let mut col_fields: Vec<(&kessel_catalog::Field, usize)> =
            Vec::with_capacity(recipe.mapping.len());
        for (fid, _source) in &recipe.mapping {
            let (idx, field) = match ot
                .fields
                .iter()
                .enumerate()
                .find(|(_, f)| f.field_id == *fid)
            {
                Some(p) => p,
                None => {
                    return OpResult::SchemaError(format!(
                        "REFRESH `{name}`: mapping references unknown \
                         field_id {fid}"
                    ))
                }
            };
            col_fields.push((field, idx));
        }
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
            for (ci, (field, idx)) in col_fields.iter().enumerate() {
                if row[ci].len() != field.kind.width() as usize {
                    return OpResult::SchemaError(format!(
                        "refresh: column `{}` raw width {} != {:?} width {}",
                        field.name,
                        row[ci].len(),
                        field.kind,
                        field.kind.width()
                    ));
                }
                values[*idx] =
                    kessel_codec::value_from_raw(field.kind, &row[ci]);
            }
            let record = match kessel_codec::encode(ot, &values) {
                Ok(r) => r,
                Err(e) => {
                    return OpResult::SchemaError(format!(
                        "refresh: record encode failed: {e:?}"
                    ))
                }
            };

            // 6. Create-vs-Update via a side-effect-free point existence
            //    check through the EXISTING route path.
            // SCALING (EXT slice 1): one point existence read per
            // row. Fine for modest feeds; a follow-on can batch
            // this into a single key-set read before the Txn.
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

    #[cfg(feature = "external-sources-objstore")]
    fn do_refresh_objstore(
        &mut self,
        recipe: &kessel_catalog::ExternalRecipe,
        ot: &kessel_catalog::ObjectType,
        name: &str,
        dedup: Vec<u8>,
    ) -> OpResult {
        use kessel_catalog::ExternalAuth;
        use kessel_fetch::{ColumnMap, Format};
        use kessel_objstore::{
            sign_get, DateTime, ObjCreds, ObjGetRequest, Provider,
        };

        let (prov, scheme_len) = if recipe.url.starts_with("s3://") {
            (Provider::S3, 5)
        } else {
            (Provider::Azure, 5)
        };
        let rest = &recipe.url[scheme_len..];
        let (b_or_c, key) = match rest.split_once('/') {
            Some((b, k)) => (b.to_string(), k.to_string()),
            None => {
                return OpResult::SchemaError(format!(
                    "REFRESH `{name}`: object URL must be \
                     <scheme>://<bucket-or-container>/<key>"
                ))
            }
        };

        let creds = match &recipe.auth {
            ExternalAuth::ObjStoreEnv { provider, a_env, b_env, account } => {
                let getenv = |k: &str| std::env::var(k);
                if *provider == 1 {
                    let key_id = match getenv(a_env) {
                        Ok(v) => v,
                        Err(_) => return OpResult::SchemaError(format!(
                            "REFRESH `{name}`: env `{a_env}` not set"
                        )),
                    };
                    let secret = match getenv(b_env) {
                        Ok(v) => v,
                        Err(_) => return OpResult::SchemaError(format!(
                            "REFRESH `{name}`: env `{b_env}` not set"
                        )),
                    };
                    ObjCreds::S3 { key_id, secret }
                } else if *provider == 2 {
                    let key_b64 = match getenv(a_env) {
                        Ok(v) => v,
                        Err(_) => return OpResult::SchemaError(format!(
                            "REFRESH `{name}`: env `{a_env}` not set"
                        )),
                    };
                    ObjCreds::AzureSharedKey {
                        account: account.clone().unwrap_or_default(),
                        key_b64,
                    }
                } else {
                    return OpResult::SchemaError(format!(
                        "REFRESH `{name}`: unknown ObjStore provider code {provider}"
                    ));
                }
            }
            _ => {
                return OpResult::SchemaError(format!(
                    "REFRESH `{name}`: object-store source missing \
                     OBJSTORE credentials"
                ))
            }
        };

        // Wall-clock injected here is non-deterministic, but is the
        // SAME captured-once boundary as the router salt / the SP99 TLS
        // handshake RNG: the signed URL + headers are transport-only;
        // only the byte-deterministic kessel_codec record enters the
        // replicated log. Moving this would NOT improve determinism.
        let now = DateTime {
            secs_since_epoch: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        };
        let signed = match sign_get(
            &ObjGetRequest {
                provider: prov,
                bucket_or_container: b_or_c,
                key,
                region: recipe.region.clone(),
                endpoint: recipe.endpoint.clone(),
                creds,
            },
            now,
        ) {
            Ok(s) => s,
            Err(e) => {
                return OpResult::SchemaError(format!(
                    "REFRESH `{name}`: sign: {e}"
                ))
            }
        };

        let mut cols: Vec<ColumnMap> =
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
        }
        let format = match recipe.format {
            0 => Format::Json,
            1 => Format::Csv,
            2 => Format::Ndjson,
            3 => Format::Parquet,
            n => {
                return OpResult::SchemaError(format!(
                    "REFRESH `{name}`: unknown format code {n}"
                ))
            }
        };

        let rows = match kessel_fetch::fetch_rows_signed(
            &signed.https_url,
            &signed.headers,
            format,
            &cols,
            recipe.rows_path.as_deref(),
            kessel_fetch::DEFAULT_MAX_BODY,
        ) {
            Ok(r) => r,
            Err(e) => {
                return OpResult::SchemaError(format!("refresh: {e}"))
            }
        };
        self.materialize_external_rows(recipe, ot, name, &cols, rows, dedup)
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
        // SP-A T2 (SP155 §3.2): the four scan ops now route to
        // `Route::Scatter` instead of `Unsupported`. `Select` /
        // `QueryRows` / `SelectFields` get the `Unordered` merge;
        // `SelectSorted` gets `Sorted` with a `sort_width = 0`
        // sentinel that `Conn::scatter_read` resolves against the
        // catalog at dispatch time.
        assert!(matches!(
            r.route(&Op::Select { type_id: 1, program: vec![], limit: 7 }),
            Route::Scatter(ScatterKind::Unordered { limit: 7 })
        ));
        assert!(matches!(
            r.route(&Op::QueryRows {
                type_id: 1,
                eq_preds: vec![],
                program: vec![],
                limit: 11,
                range_preds: vec![],
            }),
            Route::Scatter(ScatterKind::Unordered { limit: 11 })
        ));
        assert!(matches!(
            r.route(&Op::SelectFields {
                type_id: 1,
                program: vec![],
                fields: vec![0],
                limit: 5,
            }),
            Route::Scatter(ScatterKind::Unordered { limit: 5 })
        ));
        match r.route(&Op::SelectSorted {
            type_id: 1,
            program: vec![],
            sort_field: 3,
            desc: true,
            offset: 10,
            limit: 4,
        }) {
            Route::Scatter(ScatterKind::Sorted {
                sort_offset, sort_width, desc, offset, limit, ..
            }) => {
                // sort_offset carries the field-id sentinel, width=0
                // sentinel means "resolve at the call site".
                assert_eq!(sort_offset, 3, "field-id passed through");
                assert_eq!(sort_width, 0, "sentinel for catalog resolve");
                assert!(desc);
                assert_eq!(offset, 10);
                assert_eq!(limit, 4);
            }
            other => panic!("SelectSorted must scatter, got {other:?}"),
        }
        // Aggregate / GroupAggregate / Join / FindBy stay
        // `Unsupported` — SP-B/SP-D/T11/non-goal per spec.
        assert!(matches!(
            r.route(&Op::Aggregate {
                type_id: 1,
                program: vec![],
                kind: 0,
                field_id: 0,
            }),
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
                rows_path: None, pagination: None, objstore: None,
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

    /// SP-A T2 (SP155 §7.1): the headline multi-shard correctness
    /// test. Spin up two deployments — a K=4 cluster behind a router
    /// and a K=1 cluster also behind a router — populate BOTH with
    /// the SAME 16 codec-encoded rows (so the per-shard placement
    /// differs but the global row set is identical), then issue
    /// `Op::SelectSorted` against both routers and assert the
    /// returned bytes are byte-identical.
    ///
    /// This locks the spec's acceptance criterion #1 + the K=1
    /// degenerate property: a scatter over K=4 produces the same
    /// answer as a single fat shard, modulo the per-row encoding
    /// (which is identical because the per-shard SMs are
    /// deterministic and the merge is byte-equivalent to a sort of
    /// the union).
    ///
    /// Test shape:
    ///   - Type `t` has one U64 column `v` (field_id=0).
    ///   - 16 rows with id=i (1..=16) and v=i (1..=16).
    ///   - The ascending `SelectSorted` over the full set yields rows
    ///     in `v` order — and since v=id here, the ids 1..=16 each.
    ///   - Both K=1 and K=4 routers must return the identical
    ///     `OpResult::Got(bytes)`.
    ///
    /// The K=1-vs-K=4 byte-identical property is the killer SP-A
    /// correctness check (spec §7.2 property test parameterized over
    /// K; T5 widens this to random data + K∈{1,2,4,8,16}).
    #[test]
    fn scatter_select_sorted_k4_matches_k1_byte_identical() {
        use kessel_catalog::{decode_type_def, ObjectType};
        use kessel_codec::{encode, Value};
        use kessel_expr::Program;

        // ---- helper: build a router with K shards over real sockets ----
        fn spawn_k_router(k: usize, tag: &str) -> (SocketAddr, Arc<Router>) {
            let mut shards: Vec<Vec<String>> = Vec::with_capacity(k);
            for i in 0..k {
                shards.push(spawn_shard(&format!("{tag}-{i}")));
            }
            let router = Arc::new(Router::new(shards));
            let rl = TcpListener::bind("127.0.0.1:0").unwrap();
            let raddr = rl.local_addr().unwrap();
            {
                let r = router.clone();
                std::thread::spawn(move || serve_router(rl, r));
            }
            (raddr, router)
        }

        let (k1_addr, _k1_router) = spawn_k_router(1, "sp-a-k1");
        let (k4_addr, _k4_router) = spawn_k_router(4, "sp-a-k4");

        // Both deployments have 1×3 + 4×3 = 15 VSR nodes; let them
        // settle their leadership.
        std::thread::sleep(Duration::from_millis(2400));

        // ---- create the same type on both deployments ----
        let mut c_k1 = Client::connect(k1_addr).unwrap();
        let mut c_k4 = Client::connect(k4_addr).unwrap();
        let type_def_bytes = encode_type_def(
            "t",
            &[Field {
                field_id: 0,
                name: "v".into(),
                kind: FieldKind::U64,
                nullable: false,
            }],
        );
        for c in [&mut c_k1, &mut c_k4] {
            assert_eq!(
                c.call(&Op::CreateType { def: type_def_bytes.clone() })
                    .unwrap(),
                OpResult::TypeCreated(1)
            );
        }

        // ---- build the codec-shaped record for v=i ----
        // `ObjectType::from_def` is the canonical "minimal type for
        // codec encode/decode" — enough for record encoding, no
        // index/constraint state. `schema_ver` defaults to 1, which
        // matches the freshly-created type on shard 0 (the SM
        // increments it on each CreateType, starting from 1).
        let (name, fields) = decode_type_def(&type_def_bytes).unwrap();
        let ot = ObjectType::from_def(name, fields);
        let make_record = |v: u64| -> Vec<u8> {
            encode(&ot, &[Value::Uint(v as u128)]).unwrap()
        };

        // ---- insert the same 16 rows into both deployments ----
        let n: u128 = 16;
        for i in 1..=n {
            let id = ObjectId::from_u128(i);
            let rec = make_record(i as u64);
            for c in [&mut c_k1, &mut c_k4] {
                assert_eq!(
                    c.call(&Op::Create {
                        type_id: 1,
                        id,
                        record: rec.clone(),
                    })
                    .unwrap(),
                    OpResult::Ok,
                    "Create row {i} via the router must Ok"
                );
            }
        }

        // ---- the scatter: SelectSorted ascending by `v` ----
        // NB: the SM reassigns field_ids 1..=n at CreateType time
        // (deterministic); the wire request side passes field_id=0
        // but the catalog stores it as field_id=1. SelectSorted's
        // sort_field references the assigned id.
        let always_true = Program::new().push_int(1).bytes();
        let sorted_op = Op::SelectSorted {
            type_id: 1,
            program: always_true.clone(),
            sort_field: 1,
            desc: false,
            offset: 0,
            limit: 0, // all rows
        };
        let r_k1 = c_k1.call(&sorted_op).unwrap();
        let r_k4 = c_k4.call(&sorted_op).unwrap();

        // ---- assert byte-identical ----
        let bytes_k1 = match &r_k1 {
            OpResult::Got(b) => b,
            other => panic!("K=1 SelectSorted must Got, got {other:?}"),
        };
        let bytes_k4 = match &r_k4 {
            OpResult::Got(b) => b,
            other => panic!("K=4 SelectSorted must Got, got {other:?}"),
        };
        assert_eq!(
            bytes_k1, bytes_k4,
            "SP-A: SelectSorted over K=4 must be byte-identical to K=1; \
             K=1 len={} K=4 len={}",
            bytes_k1.len(),
            bytes_k4.len(),
        );

        // ---- spot-check: the result has all 16 rows ----
        // The output is `[u32 rowlen][record]*`. Count rows by walking
        // length prefixes; assert the v=i ascending order is preserved.
        let mut pos = 0usize;
        let mut count = 0u64;
        let mut last_v: i128 = -1; // -1 because v starts at 1
        while pos < bytes_k4.len() {
            let len = u32::from_le_bytes(
                bytes_k4[pos..pos + 4].try_into().unwrap(),
            ) as usize;
            pos += 4;
            let rec = &bytes_k4[pos..pos + len];
            pos += len;
            // Field 0 (U64 v) lives at HEADER_BYTES offset, 8 bytes wide.
            let v = u64::from_le_bytes(
                rec[kessel_catalog::HEADER_BYTES
                    ..kessel_catalog::HEADER_BYTES + 8]
                    .try_into()
                    .unwrap(),
            );
            assert!(
                (v as i128) > last_v,
                "ascending sort violated at row {count}: v={v} ≤ \
                 last_v={last_v}"
            );
            last_v = v as i128;
            count += 1;
        }
        assert_eq!(count, n as u64, "expected {n} rows, saw {count}");

        // ---- and the unordered scan also works across shards ----
        // `Op::Select` lowers to ScatterKind::Unordered. The result
        // length must equal the K=1 single-shard length; concrete
        // bytes may differ from K=4 (per-shard concat order vs single
        // shard order), so just lock count + total length.
        let select_op = Op::Select {
            type_id: 1,
            program: always_true,
            limit: 0,
        };
        let s_k1 = c_k1.call(&select_op).unwrap();
        let s_k4 = c_k4.call(&select_op).unwrap();
        let (s_k1_b, s_k4_b) = match (&s_k1, &s_k4) {
            (OpResult::Got(a), OpResult::Got(b)) => (a, b),
            other => panic!("Select must Got on both, got {other:?}"),
        };
        assert_eq!(
            s_k1_b.len(),
            s_k4_b.len(),
            "Select over K=4 must return the same total bytes as K=1"
        );
    }

    /// SP-A T3 (SP155 §7.1 widening): multi-shard integration tests for
    /// the other three scan ops — `Op::QueryRows`, `Op::SelectFields`,
    /// `Op::Select` (the full-table scan) — across K=1 vs K=4 over
    /// real sockets. Each scan op lowers to `ScatterKind::Unordered`
    /// via the router; per spec §3.6 the multiset of rows is K-invariant
    /// even though the byte sequence is shard-id-ordered concat.
    ///
    /// Shared deployment shape: one K=1 + one K=4 cluster (= 15 VSR
    /// nodes + 2 routers), 16 rows of a simple `(v: U64)` type. Each
    /// op is dispatched to both clusters and the result is asserted as:
    ///
    ///   - **`Op::Select`**: total bytes length matches K=1 (already
    ///     covered by the SelectSorted test; here we lock the
    ///     multiset of records as a `BTreeSet` is equal).
    ///   - **`Op::QueryRows`**: with no eq_preds / range_preds + an
    ///     always-true `program`, behaves as a `Select` over the type.
    ///     Multiset equal to K=1.
    ///   - **`Op::SelectFields`**: project just `v` (field_id=1 after
    ///     SM-side assignment); each returned row is exactly 8 bytes.
    ///     Multiset of 8-byte rows == K=1's.
    ///
    /// Because unordered scatter's *byte* sequence depends on
    /// per-shard placement (shard-id-ordered concat per SP155 §3.6),
    /// we lock the multiset rather than byte-identity. The
    /// byte-identical sorted case is covered separately by
    /// `scatter_select_sorted_k4_matches_k1_byte_identical`.
    #[test]
    fn scatter_unordered_ops_k4_match_k1_multiset() {
        use kessel_catalog::{decode_type_def, ObjectType};
        use kessel_codec::{encode, Value};
        use kessel_expr::Program;
        use std::collections::BTreeSet;

        // ---- helper: build a router with K shards over real sockets ----
        fn spawn_k_router(k: usize, tag: &str) -> SocketAddr {
            let mut shards: Vec<Vec<String>> = Vec::with_capacity(k);
            for i in 0..k {
                shards.push(spawn_shard(&format!("{tag}-{i}")));
            }
            let router = Arc::new(Router::new(shards));
            let rl = TcpListener::bind("127.0.0.1:0").unwrap();
            let raddr = rl.local_addr().unwrap();
            {
                let r = router.clone();
                std::thread::spawn(move || serve_router(rl, r));
            }
            raddr
        }

        let k1_addr = spawn_k_router(1, "sp-a-t3-k1");
        let k4_addr = spawn_k_router(4, "sp-a-t3-k4");

        // 1×3 + 4×3 = 15 VSR nodes; let them settle.
        std::thread::sleep(Duration::from_millis(2400));

        // ---- create the same type on both deployments ----
        let mut c_k1 = Client::connect(k1_addr).unwrap();
        let mut c_k4 = Client::connect(k4_addr).unwrap();
        let type_def_bytes = encode_type_def(
            "t",
            &[Field {
                field_id: 0,
                name: "v".into(),
                kind: FieldKind::U64,
                nullable: false,
            }],
        );
        for c in [&mut c_k1, &mut c_k4] {
            assert_eq!(
                c.call(&Op::CreateType { def: type_def_bytes.clone() })
                    .unwrap(),
                OpResult::TypeCreated(1)
            );
        }
        let (name, fields) = decode_type_def(&type_def_bytes).unwrap();
        let ot = ObjectType::from_def(name, fields);
        let make_record =
            |v: u64| -> Vec<u8> { encode(&ot, &[Value::Uint(v as u128)]).unwrap() };

        // ---- insert the same 16 rows into both deployments ----
        let n: u128 = 16;
        for i in 1..=n {
            let id = ObjectId::from_u128(i);
            let rec = make_record(i as u64);
            for c in [&mut c_k1, &mut c_k4] {
                assert_eq!(
                    c.call(&Op::Create {
                        type_id: 1,
                        id,
                        record: rec.clone(),
                    })
                    .unwrap(),
                    OpResult::Ok,
                );
            }
        }

        // Parse a `[u32 rowlen][record]*` payload into a multiset of rows.
        fn payload_to_multiset(bytes: &[u8]) -> BTreeSet<Vec<u8>> {
            let mut set = BTreeSet::new();
            let mut p = 0;
            while p < bytes.len() {
                let len = u32::from_le_bytes(
                    bytes[p..p + 4].try_into().unwrap(),
                ) as usize;
                p += 4;
                set.insert(bytes[p..p + len].to_vec());
                p += len;
            }
            set
        }

        let always_true = Program::new().push_int(1).bytes();

        // ---- (1) Op::Select multiset K=1 == K=4 ----
        let r_k1 = c_k1
            .call(&Op::Select {
                type_id: 1,
                program: always_true.clone(),
                limit: 0,
            })
            .unwrap();
        let r_k4 = c_k4
            .call(&Op::Select {
                type_id: 1,
                program: always_true.clone(),
                limit: 0,
            })
            .unwrap();
        let (b_k1, b_k4) = match (&r_k1, &r_k4) {
            (OpResult::Got(a), OpResult::Got(b)) => (a, b),
            other => panic!("Select must Got on both, got {other:?}"),
        };
        let set_k1 = payload_to_multiset(b_k1);
        let set_k4 = payload_to_multiset(b_k4);
        assert_eq!(set_k1.len(), n as usize, "Select K=1 must have {n} rows");
        assert_eq!(
            set_k1, set_k4,
            "Op::Select: K=4 scatter multiset must equal K=1's"
        );

        // ---- (2) Op::QueryRows multiset K=1 == K=4 ----
        let q_k1 = c_k1
            .call(&Op::QueryRows {
                type_id: 1,
                eq_preds: vec![],
                program: always_true.clone(),
                limit: 0,
                range_preds: vec![],
            })
            .unwrap();
        let q_k4 = c_k4
            .call(&Op::QueryRows {
                type_id: 1,
                eq_preds: vec![],
                program: always_true.clone(),
                limit: 0,
                range_preds: vec![],
            })
            .unwrap();
        let (b_k1, b_k4) = match (&q_k1, &q_k4) {
            (OpResult::Got(a), OpResult::Got(b)) => (a, b),
            other => panic!("QueryRows must Got on both, got {other:?}"),
        };
        let q_set_k1 = payload_to_multiset(b_k1);
        let q_set_k4 = payload_to_multiset(b_k4);
        assert_eq!(
            q_set_k1.len(),
            n as usize,
            "QueryRows K=1 must have {n} rows"
        );
        assert_eq!(
            q_set_k1, q_set_k4,
            "Op::QueryRows: K=4 scatter multiset must equal K=1's"
        );
        // Bonus: QueryRows over a Select-shaped query returns the same
        // row multiset as Select itself (both are "all rows" scans).
        assert_eq!(
            set_k4, q_set_k4,
            "QueryRows(all-true) and Select(all-true) must yield the \
             same multiset on K=4"
        );

        // ---- (3) Op::SelectFields multiset K=1 == K=4 ----
        // Project just `v` (the SM assigns field_id=1 to the 0th field
        // at CreateType time; see the SelectSorted test above).
        let f_k1 = c_k1
            .call(&Op::SelectFields {
                type_id: 1,
                program: always_true.clone(),
                fields: vec![1],
                limit: 0,
            })
            .unwrap();
        let f_k4 = c_k4
            .call(&Op::SelectFields {
                type_id: 1,
                program: always_true,
                fields: vec![1],
                limit: 0,
            })
            .unwrap();
        let (b_k1, b_k4) = match (&f_k1, &f_k4) {
            (OpResult::Got(a), OpResult::Got(b)) => (a, b),
            other => panic!("SelectFields must Got on both, got {other:?}"),
        };
        let f_set_k1 = payload_to_multiset(b_k1);
        let f_set_k4 = payload_to_multiset(b_k4);
        assert_eq!(
            f_set_k1.len(),
            n as usize,
            "SelectFields K=1 must have {n} rows"
        );
        assert_eq!(
            f_set_k1, f_set_k4,
            "Op::SelectFields: K=4 scatter multiset must equal K=1's"
        );
        // Each projected row is exactly 8 bytes (the U64 v field).
        for row in &f_set_k4 {
            assert_eq!(
                row.len(),
                8,
                "SelectFields projection: each row is the 8-byte U64 v"
            );
        }
    }
}
