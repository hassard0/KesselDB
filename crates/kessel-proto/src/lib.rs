//! kessel-proto: wire/log types, little-endian codec primitives, and a
//! deterministic PRNG. Dependency-free on purpose — determinism is a feature.

#![forbid(unsafe_code)]

pub type TypeId = u32;
pub type OpNumber = u64;
pub type ClientId = u128;

/// 128-bit caller-supplied object identifier. The engine never generates ids
/// (that would introduce nondeterminism into the state machine).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct ObjectId(pub [u8; 16]);

impl ObjectId {
    pub fn from_u128(v: u128) -> Self {
        ObjectId(v.to_le_bytes())
    }
    pub fn as_u128(&self) -> u128 {
        u128::from_le_bytes(self.0)
    }
}

/// One query predicate (Sub-project 5). `op`: 0 = Eq, 1 = Ge (>=),
/// 2 = Le (<=). `value` is the field value (width-normalized by the engine).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pred {
    pub field_id: u16,
    pub op: u8,
    pub value: Vec<u8>,
}

/// Operations applied by the deterministic state machine. Payloads are opaque
/// bytes here so `kessel-proto` stays schema-agnostic; `kessel-catalog` /
/// `kessel-codec` give them meaning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    CreateType { def: Vec<u8> },
    AlterTypeAddField { type_id: TypeId, field: Vec<u8> },
    Create { type_id: TypeId, id: ObjectId, record: Vec<u8> },
    Update { type_id: TypeId, id: ObjectId, record: Vec<u8> },
    Delete { type_id: TypeId, id: ObjectId },
    GetById { type_id: TypeId, id: ObjectId },
    /// Read a variable-length overflow blob by its deterministic handle
    /// (Sub-project 2). Write side rides inside `Create`/`Update` records.
    GetBlob { handle: u64 },
    /// Declare an equality secondary index on a field; backfills existing
    /// rows deterministically (Sub-project 3).
    CreateIndex { type_id: TypeId, field_id: u16 },
    /// Equality lookup: returns concatenated 16-byte object ids of every row
    /// whose indexed field equals `value` (Sub-project 3).
    FindBy { type_id: TypeId, field_id: u16, value: Vec<u8> },
    /// Add a UNIQUE constraint on a field (Sub-project 4): ensures/creates an
    /// index, validates current data, then enforces on future writes.
    AddUnique { type_id: TypeId, field_id: u16 },
    /// Conjunctive query (Sub-project 5): returns concatenated 16-byte object
    /// ids of rows matching ALL predicates. The planner intersects indexed
    /// equality predicates and filter-scans the rest.
    Query { type_id: TypeId, preds: Vec<Pred> },
    /// Add a foreign-key constraint (Sub-project 6): `field_id`'s value
    /// (padded to 16 bytes) must be an existing object id of
    /// `ref_type_id`. Validates current data before enabling.
    AddForeignKey { type_id: TypeId, field_id: u16, ref_type_id: TypeId, on_delete: u8 },
    /// Add a CHECK constraint (Sub-project 7): a compiled kessel-expr program
    /// that must evaluate true for every written row. Validates current data.
    AddCheck { type_id: TypeId, program: Vec<u8> },
    /// Add a before-write trigger (Sub-project 8): a compiled kessel-expr
    /// program run on each Create/Update; may mutate the record or reject it.
    AddTrigger { type_id: TypeId, program: Vec<u8> },
    /// Atomic transaction (Sub-project 9): apply every inner op all-or-
    /// nothing. Any failure rolls the whole batch back. Replicated as one op.
    Txn { ops: Vec<Op> },
    /// Boolean query (Sub-project 14): returns concatenated 16-byte object
    /// ids of rows for which the kessel-expr `program` evaluates true.
    /// Arbitrary AND/OR/NOT — a filtered scan, read-only & deterministic.
    QueryExpr { type_id: TypeId, program: Vec<u8> },
    /// Add an order-preserving range index on a field (Sub-project 15);
    /// backfills existing rows. Enables sub-linear `FindRange`.
    AddOrderedIndex { type_id: TypeId, field_id: u16 },
    /// Sub-linear inclusive range scan over an order-indexed field: returns
    /// concatenated 16-byte ids of rows with `lo <= field <= hi`.
    FindRange { type_id: TypeId, field_id: u16, lo: Vec<u8>, hi: Vec<u8> },
    /// Filtered row query (Sub-project 18): scan, keep rows where the
    /// kessel-expr `program` is true, return up to `limit` rows as
    /// length-prefixed record blobs. Read-only & deterministic.
    Select { type_id: TypeId, program: Vec<u8>, limit: u32 },
    /// Index-accelerated row query (Sub-project 32): `eq_preds` are
    /// `(field_id, value)` equality predicates; any on an indexed field are
    /// intersected via the index to narrow candidates (else a full scan).
    /// `range_preds` (SP70) are `(field_id, op, value)` half-range hints
    /// (`op` 0=`>` 1=`>=` 2=`<` 3=`<=`) on order-indexed fields, narrowed
    /// via the order index. `program` (the full WHERE) then verifies every
    /// candidate, so the result is identical to `Select` regardless of the
    /// candidate set — the indexes only accelerate. `range_preds` is
    /// encoded *after* `limit` so an older frame (no range hints) decodes
    /// to an empty list and behaves exactly as before (wire-compatible).
    /// Returns up to `limit` rows as `[u32 len][record]*`.
    QueryRows {
        type_id: TypeId,
        eq_preds: Vec<(u16, Vec<u8>)>,
        program: Vec<u8>,
        limit: u32,
        range_preds: Vec<(u16, u8, Vec<u8>)>,
    },
    /// Aggregate (Sub-project 20) over rows matching `program`:
    /// `kind` 0=COUNT, 1=SUM, 2=MIN, 3=MAX of `field_id` (numeric).
    /// Result returned as a 16-byte little-endian i128 in `Got`.
    Aggregate { type_id: TypeId, program: Vec<u8>, kind: u8, field_id: u16 },
    /// Projection (Sub-project 21): like `Select` but each returned row is
    /// only the concatenated bytes of `fields` (in order), not the whole
    /// record. Result = `[u32 rowlen][row]*`. Read-only & deterministic.
    SelectFields { type_id: TypeId, program: Vec<u8>, fields: Vec<u16>, limit: u32 },
    /// GROUP BY aggregate (Sub-project 22): over rows matching `program`,
    /// group by `group_field`'s value and compute `kind` (0 COUNT / 1 SUM /
    /// 2 MIN / 3 MAX) of `agg_field` per group. Result =
    /// `[u32 ngroups]` then per group `[u32 keylen][key][16B i128 LE]`,
    /// groups in ascending key order (deterministic). Read-only.
    GroupAggregate {
        type_id: TypeId,
        program: Vec<u8>,
        group_field: u16,
        kind: u8,
        agg_field: u16,
    },
    /// Schema introspection (Sub-project 34): returns the table's serialized
    /// `(name, fields)` definition so a client can decode `SELECT` rows.
    Describe { type_id: TypeId },
    /// Destructive DDL (Sub-project 54): drop a table — remove its rows,
    /// its own indexes, and the type from the catalog. Rejected (no
    /// effect) if another table's foreign key still references it.
    /// Deterministic; replicated as one op.
    DropType { type_id: TypeId },
    /// Destructive DDL (SP74): drop the secondary index(es) on exactly
    /// `fields`. One field ⇒ its equality (and UNIQUE) and/or range
    /// index; multiple ⇒ the composite index with that exact field
    /// list. Index entries are deleted and the catalog updated; query
    /// results are unchanged (the planner falls back to a verified
    /// scan), only un-accelerated. `NotFound` if no such index.
    DropIndex { type_id: TypeId, fields: Vec<u16> },
    /// Destructive ALTER (SP75): physically remove a column. Every row
    /// is re-encoded without it, the schema shrinks, and the column's
    /// own indexes (and any composite that referenced it) are dropped —
    /// so nothing downstream needs a "dropped" special case.
    DropField { type_id: TypeId, field_id: u16 },
    /// ALTER … RENAME COLUMN (SP75): catalog-only; indexes are keyed by
    /// field id so data and indexes are untouched.
    RenameField { type_id: TypeId, field_id: u16, name: String },
    /// Declare an external source (external-sources feature): a named
    /// HTTP-backed virtual table. `type_def` is `encode_type_def(name,
    /// fields)` for the backing type; `url`/`format` (0 JSON, 1 CSV)
    /// describe the upstream; `key_field_id` is the dedup key; `auth_*`
    /// describe optional auth (`auth_kind` 0 None / 1 BearerEnv /
    /// 2 HeaderEnv); `mapping` is `(field_id, source path)` pairs.
    CreateExternalSource {
        name: String,
        /// `encode_type_def(name, fields)` for the backing type.
        type_def: Vec<u8>,
        url: String,
        format: u8,        // 0 JSON, 1 CSV
        key_field_id: u16,
        auth_kind: u8,     // 0 None, 1 BearerEnv, 2 HeaderEnv
        auth_a: String,    // BearerEnv: env name | HeaderEnv: header
        auth_b: String,    // HeaderEnv: env name (else "")
        mapping: Vec<(u16, String)>,
    },
    /// Drop a declared external source (external-sources feature).
    DropExternalSource { name: String },
    /// Trigger a re-fetch of an external source (external-sources feature).
    RefreshExternalSource { name: String },
    /// Balance-guard helper (SP77): a named non-negative invariant on a
    /// signed numeric column (`field >= 0`). Implemented as a `CHECK`
    /// (reusing that proven enforcement on every write, incl. inside a
    /// transaction) — the helper is the ergonomic, validated surface.
    AddBalanceGuard { type_id: TypeId, field_id: u16 },
    /// Global sequencer (SP79, cross-shard slice 2): atomically assign
    /// the next gap-free sequence number and store `payload` (a
    /// cross-shard transaction descriptor) under it, in ONE replicated
    /// op. Reply `Got(seq u64 LE)`. The sequencer is an ordinary VSR
    /// group, so this total order is linearizable and failover-safe;
    /// the counter lives in a reserved keyspace so it is part of the
    /// replicated digest and WAL-recovered.
    SeqAppend { payload: Vec<u8> },
    /// Read the ordered descriptor log from `from` (inclusive), up to
    /// `limit` entries (0 = all). Reply `Got([u64 seq][u32 len][payload])*`.
    SeqRead { from: u64, limit: u32 },
    /// Apply this shard's slice of the cross-shard transaction at global
    /// `seq` (SP80, slice 3). Idempotent and strictly in-order: a shard
    /// processes every global seq in order (its slice, or empty to just
    /// advance), tracking a cursor in a reserved keyspace. The ordered
    /// sequencer log is the commit point — no 2PC, no locks.
    XshardApply { seq: u64, ops: Vec<Op> },
    /// Exactly-once sequencer append (SP81): if `key` was seen before,
    /// return its already-assigned seq (no new entry); else assign +
    /// store + remember `key`. The key→seq map lives in the digest, so
    /// a client/router retry is exactly-once and crash-safe.
    SeqAppendOnce { key: Vec<u8>, payload: Vec<u8> },
    /// Phase 1 of deterministic cross-shard commit (SP81): dry-run this
    /// shard's slice against committed state, persist a verdict for
    /// `seq` (idempotent), apply NOTHING. Reply `Got([1])`=would-commit
    /// / `Got([0])`=would-abort. The verdict is a pure function of
    /// durable state, so every router re-derives the same decision.
    XshardDecide { seq: u64, ops: Vec<Op> },
    /// Phase 2 (SP81): if `commit`, apply the slice atomically and
    /// advance the cursor; else just advance (deterministic skip).
    /// Cursor-idempotent and strictly in seq order, like `XshardApply`.
    XshardCommit { seq: u64, ops: Vec<Op>, commit: bool },
    /// Deterministic server-side read-modify-write (SP84): splice each
    /// `(field_id, raw bytes)` into the row's current record and write
    /// it back, as ONE replicated op. Unlike the connection-layer SQL
    /// `UPDATE` RMW this composes inside `Op::Txn` (reads are
    /// overlay-aware ⇒ read-your-writes). `NotFound` if absent.
    UpdateSet { type_id: TypeId, id: ObjectId, sets: Vec<(u16, Vec<u8>)> },
    /// Inner equi-join (Sub-project 36): rows where
    /// `left.left_field == right.right_field` (raw fixed-width bytes).
    /// Returns up to `limit` joined rows as
    /// `[u32 total][u32 left_len][left rec][right rec]*`, deterministic
    /// (left key order, then right scan order). Read-only.
    Join {
        left_type: TypeId,
        right_type: TypeId,
        left_field: u16,
        right_field: u16,
        limit: u32,
    },
    /// Add a composite (multi-field) equality index (Sub-project 27);
    /// backfills existing rows.
    AddCompositeIndex { type_id: TypeId, fields: Vec<u16> },
    /// Equality lookup on a composite index: `fields` identifies the index,
    /// `values` are the per-field query values (in the same order).
    /// Returns concatenated 16-byte object ids.
    FindByComposite { type_id: TypeId, fields: Vec<u16>, values: Vec<Vec<u8>> },
    /// Sorted/paginated query (Sub-project 23): rows matching `program`,
    /// ordered by `sort_field` (`desc` for descending; ties broken by
    /// object id for determinism), then `offset` skipped and at most
    /// `limit` returned (0 = unlimited). Result = `[u32 rowlen][record]*`.
    SelectSorted {
        type_id: TypeId,
        program: Vec<u8>,
        sort_field: u16,
        desc: bool,
        offset: u32,
        limit: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpResult {
    Ok,
    Got(Vec<u8>),
    Exists,
    NotFound,
    TypeCreated(TypeId),
    SchemaError(String),
    /// A built-in constraint (NOT NULL / UNIQUE) rejected the write
    /// (Sub-project 4). Deterministic — counts as a committed op result.
    Constraint(String),
    /// This node cannot serve the request right now (not the active
    /// primary, or mid view-change) and held no cached result for it.
    /// NOT a committed result — a transport-level "try another node"
    /// signal so a cluster client rotates to the primary (Sub-project 42).
    Unavailable,
    /// Connection-level auth failed (missing/incorrect shared-secret
    /// token). Transport-level, not a committed result (Sub-project 43).
    Unauthorized,
}

impl OpResult {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        match self {
            OpResult::Ok => b.push(0),
            OpResult::Got(v) => {
                b.push(1);
                codec::put_bytes(&mut b, v);
            }
            OpResult::Exists => b.push(2),
            OpResult::NotFound => b.push(3),
            OpResult::TypeCreated(t) => {
                b.push(4);
                codec::put_u32(&mut b, *t);
            }
            OpResult::SchemaError(s) => {
                b.push(5);
                codec::put_bytes(&mut b, s.as_bytes());
            }
            OpResult::Constraint(s) => {
                b.push(6);
                codec::put_bytes(&mut b, s.as_bytes());
            }
            OpResult::Unavailable => b.push(7),
            OpResult::Unauthorized => b.push(8),
        }
        b
    }

    pub fn decode(buf: &[u8]) -> Option<OpResult> {
        let mut c = codec::Cursor::new(buf);
        Some(match c.u8()? {
            0 => OpResult::Ok,
            1 => OpResult::Got(c.bytes()?),
            2 => OpResult::Exists,
            3 => OpResult::NotFound,
            4 => OpResult::TypeCreated(c.u32()?),
            5 => OpResult::SchemaError(String::from_utf8_lossy(&c.bytes()?).into_owned()),
            6 => OpResult::Constraint(String::from_utf8_lossy(&c.bytes()?).into_owned()),
            7 => OpResult::Unavailable,
            8 => OpResult::Unauthorized,
            _ => return None,
        })
    }
}

impl Op {
    /// Discriminant tag used in WAL frames and the wire protocol.
    pub fn kind(&self) -> u8 {
        match self {
            Op::CreateType { .. } => 1,
            Op::AlterTypeAddField { .. } => 2,
            Op::Create { .. } => 3,
            Op::Update { .. } => 4,
            Op::Delete { .. } => 5,
            Op::GetById { .. } => 6,
            Op::GetBlob { .. } => 7,
            Op::CreateIndex { .. } => 8,
            Op::FindBy { .. } => 9,
            Op::AddUnique { .. } => 10,
            Op::Query { .. } => 11,
            Op::AddForeignKey { .. } => 12,
            Op::AddCheck { .. } => 13,
            Op::AddTrigger { .. } => 14,
            Op::Txn { .. } => 15,
            Op::QueryExpr { .. } => 16,
            Op::AddOrderedIndex { .. } => 17,
            Op::FindRange { .. } => 18,
            Op::Select { .. } => 19,
            Op::QueryRows { .. } => 26,
            Op::Describe { .. } => 27,
            Op::DropType { .. } => 29,
            Op::DropIndex { .. } => 30,
            Op::DropField { .. } => 31,
            Op::RenameField { .. } => 32,
            Op::AddBalanceGuard { .. } => 33,
            Op::SeqAppend { .. } => 34,
            Op::SeqRead { .. } => 35,
            Op::XshardApply { .. } => 36,
            Op::SeqAppendOnce { .. } => 37,
            Op::XshardDecide { .. } => 38,
            Op::XshardCommit { .. } => 39,
            Op::UpdateSet { .. } => 40,
            Op::CreateExternalSource { .. } => 41,
            Op::DropExternalSource { .. } => 42,
            Op::RefreshExternalSource { .. } => 43,
            Op::Join { .. } => 28,
            Op::Aggregate { .. } => 20,
            Op::SelectFields { .. } => 21,
            Op::GroupAggregate { .. } => 22,
            Op::SelectSorted { .. } => 23,
            Op::AddCompositeIndex { .. } => 24,
            Op::FindByComposite { .. } => 25,
        }
    }

    /// True if applying this op can change committed state (writes,
    /// DDL, sequencer append, cross-shard apply/decide/commit).
    /// Reads (`Get*`/`Find*`/`Query*`/`Select*`/`Aggregate*`/
    /// `Describe`/`SeqRead`/`Join`) return `false` — re-running them is
    /// side-effect-free, so the SP94 crash-recovery replay guard must
    /// never short-circuit them (they must always return real data).
    pub fn is_mutating(&self) -> bool {
        // External-source ops (CreateExternalSource / DropExternalSource /
        // RefreshExternalSource) are deliberately absent from the read-op
        // list below, so this negative match correctly classifies them as
        // mutating (they change catalog/committed state).
        !matches!(
            self,
            Op::GetById { .. }
                | Op::GetBlob { .. }
                | Op::FindBy { .. }
                | Op::Query { .. }
                | Op::QueryExpr { .. }
                | Op::FindRange { .. }
                | Op::Select { .. }
                | Op::QueryRows { .. }
                | Op::Describe { .. }
                | Op::SeqRead { .. }
                | Op::Join { .. }
                | Op::Aggregate { .. }
                | Op::SelectFields { .. }
                | Op::GroupAggregate { .. }
                | Op::SelectSorted { .. }
                | Op::FindByComposite { .. }
        )
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.push(self.kind());
        match self {
            Op::CreateType { def } => codec::put_bytes(&mut b, def),
            Op::AlterTypeAddField { type_id, field } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_bytes(&mut b, field);
            }
            Op::Create { type_id, id, record }
            | Op::Update { type_id, id, record } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&id.0);
                codec::put_bytes(&mut b, record);
            }
            Op::Delete { type_id, id } | Op::GetById { type_id, id } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&id.0);
            }
            Op::GetBlob { handle } => codec::put_u64(&mut b, *handle),
            Op::CreateIndex { type_id, field_id } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&field_id.to_le_bytes());
            }
            Op::FindBy { type_id, field_id, value } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&field_id.to_le_bytes());
                codec::put_bytes(&mut b, value);
            }
            Op::AddUnique { type_id, field_id } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&field_id.to_le_bytes());
            }
            Op::Query { type_id, preds } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_u32(&mut b, preds.len() as u32);
                for p in preds {
                    b.extend_from_slice(&p.field_id.to_le_bytes());
                    b.push(p.op);
                    codec::put_bytes(&mut b, &p.value);
                }
            }
            Op::AddForeignKey { type_id, field_id, ref_type_id, on_delete } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&field_id.to_le_bytes());
                codec::put_u32(&mut b, *ref_type_id);
                b.push(*on_delete);
            }
            Op::AddCheck { type_id, program } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_bytes(&mut b, program);
            }
            Op::AddTrigger { type_id, program } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_bytes(&mut b, program);
            }
            Op::Txn { ops } => {
                codec::put_u32(&mut b, ops.len() as u32);
                for o in ops {
                    codec::put_bytes(&mut b, &o.encode()); // length-prefixed
                }
            }
            Op::QueryExpr { type_id, program } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_bytes(&mut b, program);
            }
            Op::AddOrderedIndex { type_id, field_id } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&field_id.to_le_bytes());
            }
            Op::FindRange { type_id, field_id, lo, hi } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&field_id.to_le_bytes());
                codec::put_bytes(&mut b, lo);
                codec::put_bytes(&mut b, hi);
            }
            Op::Select { type_id, program, limit } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_bytes(&mut b, program);
                codec::put_u32(&mut b, *limit);
            }
            Op::SeqAppend { payload } => codec::put_bytes(&mut b, payload),
            Op::SeqRead { from, limit } => {
                b.extend_from_slice(&from.to_le_bytes());
                codec::put_u32(&mut b, *limit);
            }
            Op::XshardApply { seq, ops }
            | Op::XshardDecide { seq, ops } => {
                b.extend_from_slice(&seq.to_le_bytes());
                codec::put_u32(&mut b, ops.len() as u32);
                for o in ops {
                    codec::put_bytes(&mut b, &o.encode());
                }
            }
            Op::XshardCommit { seq, ops, commit } => {
                b.extend_from_slice(&seq.to_le_bytes());
                b.push(*commit as u8);
                codec::put_u32(&mut b, ops.len() as u32);
                for o in ops {
                    codec::put_bytes(&mut b, &o.encode());
                }
            }
            Op::UpdateSet { type_id, id, sets } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&id.0);
                codec::put_u32(&mut b, sets.len() as u32);
                for (f, raw) in sets {
                    b.extend_from_slice(&f.to_le_bytes());
                    codec::put_bytes(&mut b, raw);
                }
            }
            Op::SeqAppendOnce { key, payload } => {
                codec::put_bytes(&mut b, key);
                codec::put_bytes(&mut b, payload);
            }
            Op::CreateExternalSource {
                name, type_def, url, format, key_field_id,
                auth_kind, auth_a, auth_b, mapping,
            } => {
                codec::put_bytes(&mut b, name.as_bytes());
                codec::put_bytes(&mut b, type_def);
                codec::put_bytes(&mut b, url.as_bytes());
                b.push(*format);
                b.extend_from_slice(&key_field_id.to_le_bytes());
                b.push(*auth_kind);
                codec::put_bytes(&mut b, auth_a.as_bytes());
                codec::put_bytes(&mut b, auth_b.as_bytes());
                codec::put_u32(&mut b, mapping.len() as u32);
                for (fid, src) in mapping {
                    b.extend_from_slice(&fid.to_le_bytes());
                    codec::put_bytes(&mut b, src.as_bytes());
                }
            }
            Op::DropExternalSource { name }
            | Op::RefreshExternalSource { name } => {
                codec::put_bytes(&mut b, name.as_bytes());
            }
            Op::Describe { type_id } | Op::DropType { type_id } => {
                codec::put_u32(&mut b, *type_id)
            }
            Op::DropField { type_id, field_id }
            | Op::AddBalanceGuard { type_id, field_id } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&field_id.to_le_bytes());
            }
            Op::RenameField { type_id, field_id, name } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&field_id.to_le_bytes());
                codec::put_bytes(&mut b, name.as_bytes());
            }
            Op::Join { left_type, right_type, left_field, right_field, limit } => {
                codec::put_u32(&mut b, *left_type);
                codec::put_u32(&mut b, *right_type);
                b.extend_from_slice(&left_field.to_le_bytes());
                b.extend_from_slice(&right_field.to_le_bytes());
                codec::put_u32(&mut b, *limit);
            }
            Op::QueryRows { type_id, eq_preds, program, limit, range_preds } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_u32(&mut b, eq_preds.len() as u32);
                for (f, v) in eq_preds {
                    b.extend_from_slice(&f.to_le_bytes());
                    codec::put_bytes(&mut b, v);
                }
                codec::put_bytes(&mut b, program);
                codec::put_u32(&mut b, *limit);
                // SP70: range hints appended last so an older (no-range)
                // frame is still a valid prefix that decodes to empty.
                if !range_preds.is_empty() {
                    codec::put_u32(&mut b, range_preds.len() as u32);
                    for (f, o, v) in range_preds {
                        b.extend_from_slice(&f.to_le_bytes());
                        b.push(*o);
                        codec::put_bytes(&mut b, v);
                    }
                }
            }
            Op::Aggregate { type_id, program, kind, field_id } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_bytes(&mut b, program);
                b.push(*kind);
                b.extend_from_slice(&field_id.to_le_bytes());
            }
            Op::SelectFields { type_id, program, fields, limit } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_bytes(&mut b, program);
                codec::put_u32(&mut b, fields.len() as u32);
                for f in fields {
                    b.extend_from_slice(&f.to_le_bytes());
                }
                codec::put_u32(&mut b, *limit);
            }
            Op::GroupAggregate { type_id, program, group_field, kind, agg_field } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_bytes(&mut b, program);
                b.extend_from_slice(&group_field.to_le_bytes());
                b.push(*kind);
                b.extend_from_slice(&agg_field.to_le_bytes());
            }
            Op::AddCompositeIndex { type_id, fields }
            | Op::DropIndex { type_id, fields } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_u32(&mut b, fields.len() as u32);
                for f in fields {
                    b.extend_from_slice(&f.to_le_bytes());
                }
            }
            Op::FindByComposite { type_id, fields, values } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_u32(&mut b, fields.len() as u32);
                for f in fields {
                    b.extend_from_slice(&f.to_le_bytes());
                }
                codec::put_u32(&mut b, values.len() as u32);
                for v in values {
                    codec::put_bytes(&mut b, v);
                }
            }
            Op::SelectSorted { type_id, program, sort_field, desc, offset, limit } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_bytes(&mut b, program);
                b.extend_from_slice(&sort_field.to_le_bytes());
                b.push(*desc as u8);
                codec::put_u32(&mut b, *offset);
                codec::put_u32(&mut b, *limit);
            }
        }
        b
    }

    pub fn decode(buf: &[u8]) -> Option<Op> {
        let mut c = codec::Cursor::new(buf);
        let kind = c.u8()?;
        let op = match kind {
            1 => Op::CreateType { def: c.bytes()? },
            2 => Op::AlterTypeAddField { type_id: c.u32()?, field: c.bytes()? },
            3 => Op::Create { type_id: c.u32()?, id: c.object_id()?, record: c.bytes()? },
            4 => Op::Update { type_id: c.u32()?, id: c.object_id()?, record: c.bytes()? },
            5 => Op::Delete { type_id: c.u32()?, id: c.object_id()? },
            6 => Op::GetById { type_id: c.u32()?, id: c.object_id()? },
            7 => Op::GetBlob { handle: c.u64()? },
            8 => Op::CreateIndex { type_id: c.u32()?, field_id: c.u16()? },
            9 => Op::FindBy { type_id: c.u32()?, field_id: c.u16()?, value: c.bytes()? },
            10 => Op::AddUnique { type_id: c.u32()?, field_id: c.u16()? },
            11 => {
                let type_id = c.u32()?;
                let n = c.u32()? as usize;
                let mut preds = Vec::with_capacity(n);
                for _ in 0..n {
                    preds.push(Pred {
                        field_id: c.u16()?,
                        op: c.u8()?,
                        value: c.bytes()?,
                    });
                }
                Op::Query { type_id, preds }
            }
            12 => Op::AddForeignKey {
                type_id: c.u32()?,
                field_id: c.u16()?,
                ref_type_id: c.u32()?,
                on_delete: c.u8()?,
            },
            13 => Op::AddCheck { type_id: c.u32()?, program: c.bytes()? },
            14 => Op::AddTrigger { type_id: c.u32()?, program: c.bytes()? },
            15 => {
                let n = c.u32()? as usize;
                let mut ops = Vec::with_capacity(n);
                for _ in 0..n {
                    let inner = c.bytes()?;
                    let o = Op::decode(&inner)?;
                    if matches!(o, Op::Txn { .. }) {
                        return None; // no nested transactions
                    }
                    ops.push(o);
                }
                Op::Txn { ops }
            }
            16 => Op::QueryExpr { type_id: c.u32()?, program: c.bytes()? },
            17 => Op::AddOrderedIndex { type_id: c.u32()?, field_id: c.u16()? },
            18 => Op::FindRange {
                type_id: c.u32()?,
                field_id: c.u16()?,
                lo: c.bytes()?,
                hi: c.bytes()?,
            },
            19 => Op::Select {
                type_id: c.u32()?,
                program: c.bytes()?,
                limit: c.u32()?,
            },
            27 => Op::Describe { type_id: c.u32()? },
            29 => Op::DropType { type_id: c.u32()? },
            30 => {
                let type_id = c.u32()?;
                let nf = c.u32()? as usize;
                let mut fields = Vec::with_capacity(nf);
                for _ in 0..nf {
                    fields.push(c.u16()?);
                }
                Op::DropIndex { type_id, fields }
            }
            31 => Op::DropField { type_id: c.u32()?, field_id: c.u16()? },
            33 => Op::AddBalanceGuard { type_id: c.u32()?, field_id: c.u16()? },
            34 => Op::SeqAppend { payload: c.bytes()? },
            35 => Op::SeqRead { from: c.u64()?, limit: c.u32()? },
            36 => {
                let seq = c.u64()?;
                let n = c.u32()? as usize;
                let mut ops = Vec::with_capacity(n);
                for _ in 0..n {
                    let inner = c.bytes()?;
                    let o = Op::decode(&inner)?;
                    if matches!(o, Op::Txn { .. } | Op::XshardApply { .. }) {
                        return None; // no nested batch ops in a slice
                    }
                    ops.push(o);
                }
                Op::XshardApply { seq, ops }
            }
            37 => Op::SeqAppendOnce {
                key: c.bytes()?,
                payload: c.bytes()?,
            },
            38 => {
                let seq = c.u64()?;
                let n = c.u32()? as usize;
                let mut ops = Vec::with_capacity(n);
                for _ in 0..n {
                    let o = Op::decode(&c.bytes()?)?;
                    if matches!(o, Op::Txn { .. } | Op::XshardApply { .. }) {
                        return None;
                    }
                    ops.push(o);
                }
                Op::XshardDecide { seq, ops }
            }
            39 => {
                let seq = c.u64()?;
                let commit = c.u8()? != 0;
                let n = c.u32()? as usize;
                let mut ops = Vec::with_capacity(n);
                for _ in 0..n {
                    let o = Op::decode(&c.bytes()?)?;
                    if matches!(o, Op::Txn { .. } | Op::XshardApply { .. }) {
                        return None;
                    }
                    ops.push(o);
                }
                Op::XshardCommit { seq, ops, commit }
            }
            40 => {
                let type_id = c.u32()?;
                let id = c.object_id()?;
                let n = c.u32()? as usize;
                let mut sets = Vec::with_capacity(n);
                for _ in 0..n {
                    sets.push((c.u16()?, c.bytes()?));
                }
                Op::UpdateSet { type_id, id, sets }
            }
            41 => {
                let name = String::from_utf8_lossy(&c.bytes()?).into_owned();
                let type_def = c.bytes()?;
                let url = String::from_utf8_lossy(&c.bytes()?).into_owned();
                let format = c.u8()?;
                let key_field_id = c.u16()?;
                let auth_kind = c.u8()?;
                let auth_a = String::from_utf8_lossy(&c.bytes()?).into_owned();
                let auth_b = String::from_utf8_lossy(&c.bytes()?).into_owned();
                let n = c.u32()? as usize;
                let mut mapping = Vec::with_capacity(n);
                for _ in 0..n {
                    let fid = c.u16()?;
                    let src = String::from_utf8_lossy(&c.bytes()?).into_owned();
                    mapping.push((fid, src));
                }
                Op::CreateExternalSource {
                    name, type_def, url, format, key_field_id,
                    auth_kind, auth_a, auth_b, mapping,
                }
            }
            42 => Op::DropExternalSource {
                name: String::from_utf8_lossy(&c.bytes()?).into_owned(),
            },
            43 => Op::RefreshExternalSource {
                name: String::from_utf8_lossy(&c.bytes()?).into_owned(),
            },
            32 => Op::RenameField {
                type_id: c.u32()?,
                field_id: c.u16()?,
                name: String::from_utf8_lossy(&c.bytes()?).into_owned(),
            },
            28 => Op::Join {
                left_type: c.u32()?,
                right_type: c.u32()?,
                left_field: c.u16()?,
                right_field: c.u16()?,
                limit: c.u32()?,
            },
            26 => {
                let type_id = c.u32()?;
                let n = c.u32()? as usize;
                let mut eq_preds = Vec::with_capacity(n);
                for _ in 0..n {
                    eq_preds.push((c.u16()?, c.bytes()?));
                }
                let program = c.bytes()?;
                let limit = c.u32()?;
                // SP70: optional trailing range hints. Absent (older
                // frame) ⇒ empty ⇒ identical behaviour to before.
                let range_preds = if c.remaining() > 0 {
                    let m = c.u32()? as usize;
                    let mut rp = Vec::with_capacity(m);
                    for _ in 0..m {
                        rp.push((c.u16()?, c.u8()?, c.bytes()?));
                    }
                    rp
                } else {
                    Vec::new()
                };
                Op::QueryRows { type_id, eq_preds, program, limit, range_preds }
            }
            20 => Op::Aggregate {
                type_id: c.u32()?,
                program: c.bytes()?,
                kind: c.u8()?,
                field_id: c.u16()?,
            },
            21 => {
                let type_id = c.u32()?;
                let program = c.bytes()?;
                let nf = c.u32()? as usize;
                let mut fields = Vec::with_capacity(nf);
                for _ in 0..nf {
                    fields.push(c.u16()?);
                }
                Op::SelectFields { type_id, program, fields, limit: c.u32()? }
            }
            22 => Op::GroupAggregate {
                type_id: c.u32()?,
                program: c.bytes()?,
                group_field: c.u16()?,
                kind: c.u8()?,
                agg_field: c.u16()?,
            },
            24 => {
                let type_id = c.u32()?;
                let nf = c.u32()? as usize;
                let mut fields = Vec::with_capacity(nf);
                for _ in 0..nf {
                    fields.push(c.u16()?);
                }
                Op::AddCompositeIndex { type_id, fields }
            }
            25 => {
                let type_id = c.u32()?;
                let nf = c.u32()? as usize;
                let mut fields = Vec::with_capacity(nf);
                for _ in 0..nf {
                    fields.push(c.u16()?);
                }
                let nv = c.u32()? as usize;
                let mut values = Vec::with_capacity(nv);
                for _ in 0..nv {
                    values.push(c.bytes()?);
                }
                Op::FindByComposite { type_id, fields, values }
            }
            23 => Op::SelectSorted {
                type_id: c.u32()?,
                program: c.bytes()?,
                sort_field: c.u16()?,
                desc: c.u8()? != 0,
                offset: c.u32()?,
                limit: c.u32()?,
            },
            _ => return None,
        };
        Some(op)
    }
}

/// Little-endian primitives, length-prefixed byte fields, CRC-32C (Castagnoli).
pub mod codec {
    use crate::ObjectId;

    pub fn put_u8(b: &mut Vec<u8>, v: u8) {
        b.push(v);
    }
    pub fn put_u32(b: &mut Vec<u8>, v: u32) {
        b.extend_from_slice(&v.to_le_bytes());
    }
    pub fn put_u64(b: &mut Vec<u8>, v: u64) {
        b.extend_from_slice(&v.to_le_bytes());
    }
    pub fn put_bytes(b: &mut Vec<u8>, v: &[u8]) {
        put_u32(b, v.len() as u32);
        b.extend_from_slice(v);
    }

    pub struct Cursor<'a> {
        buf: &'a [u8],
        pos: usize,
    }

    impl<'a> Cursor<'a> {
        pub fn new(buf: &'a [u8]) -> Self {
            Cursor { buf, pos: 0 }
        }
        pub fn u8(&mut self) -> Option<u8> {
            let v = *self.buf.get(self.pos)?;
            self.pos += 1;
            Some(v)
        }
        pub fn u16(&mut self) -> Option<u16> {
            let s = self.buf.get(self.pos..self.pos + 2)?;
            self.pos += 2;
            Some(u16::from_le_bytes(s.try_into().ok()?))
        }
        pub fn u32(&mut self) -> Option<u32> {
            let s = self.buf.get(self.pos..self.pos + 4)?;
            self.pos += 4;
            Some(u32::from_le_bytes(s.try_into().ok()?))
        }
        pub fn u64(&mut self) -> Option<u64> {
            let s = self.buf.get(self.pos..self.pos + 8)?;
            self.pos += 8;
            Some(u64::from_le_bytes(s.try_into().ok()?))
        }
        pub fn object_id(&mut self) -> Option<ObjectId> {
            let s = self.buf.get(self.pos..self.pos + 16)?;
            self.pos += 16;
            Some(ObjectId(s.try_into().ok()?))
        }
        pub fn bytes(&mut self) -> Option<Vec<u8>> {
            let n = self.u32()? as usize;
            let s = self.buf.get(self.pos..self.pos + n)?;
            self.pos += n;
            Some(s.to_vec())
        }
        pub fn remaining(&self) -> usize {
            self.buf.len() - self.pos
        }
    }

    const CRC32C_POLY: u32 = 0x82F6_3B78;

    /// CRC-32C (Castagnoli). Software table-free implementation — slow but
    /// dependency-free and bit-identical everywhere (determinism > speed for
    /// the integrity check; hot paths can swap a table later).
    pub fn crc32c(data: &[u8]) -> u32 {
        let mut crc: u32 = 0xFFFF_FFFF;
        for &byte in data {
            crc ^= byte as u32;
            for _ in 0..8 {
                let mask = (crc & 1).wrapping_neg();
                crc = (crc >> 1) ^ (CRC32C_POLY & mask);
            }
        }
        !crc
    }
}

/// Length-prefixed framing shared by the TCP server and client:
/// `[u32 little-endian length][payload]`.
pub mod wire {
    use std::io::{self, Read, Write};

    pub fn write_frame(w: &mut impl Write, payload: &[u8]) -> io::Result<()> {
        w.write_all(&(payload.len() as u32).to_le_bytes())?;
        w.write_all(payload)?;
        w.flush()
    }

    pub fn read_frame(r: &mut impl Read) -> io::Result<Vec<u8>> {
        let mut len = [0u8; 4];
        r.read_exact(&mut len)?;
        let n = u32::from_le_bytes(len) as usize;
        let mut buf = vec![0u8; n];
        r.read_exact(&mut buf)?;
        Ok(buf)
    }
}

/// Deterministic splitmix64 PRNG. Used by tests and the simulator so a single
/// `u64` seed reproduces an entire run bit-for-bit.
#[derive(Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// Uniform in `[0, n)`. `n == 0` returns 0.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }
    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let r = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&r[..chunk.len()]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_roundtrip_all_variants() {
        let id = ObjectId::from_u128(0xDEAD_BEEF_CAFE);
        let ops = vec![
            Op::CreateType { def: vec![1, 2, 3] },
            Op::AlterTypeAddField { type_id: 7, field: vec![9, 9] },
            Op::Create { type_id: 4, id, record: vec![0xAA; 130] },
            Op::Update { type_id: 4, id, record: vec![] },
            Op::Delete { type_id: 4, id },
            Op::GetById { type_id: 4, id },
            Op::GetBlob { handle: 0xABCD_1234_5678 },
            Op::CreateIndex { type_id: 4, field_id: 2 },
            Op::FindBy { type_id: 4, field_id: 2, value: vec![1, 2, 3, 4] },
            Op::AddUnique { type_id: 4, field_id: 2 },
            Op::Query {
                type_id: 4,
                preds: vec![
                    Pred { field_id: 1, op: 0, value: vec![9, 9] },
                    Pred { field_id: 2, op: 1, value: vec![] },
                ],
            },
            Op::AddForeignKey { type_id: 4, field_id: 1, ref_type_id: 2, on_delete: 2 },
            Op::AddCheck { type_id: 4, program: vec![0, 1, 2, 3] },
            Op::AddTrigger { type_id: 4, program: vec![5, 6] },
            Op::Txn {
                ops: vec![
                    Op::Create { type_id: 1, id, record: vec![1, 2] },
                    Op::Delete { type_id: 1, id },
                ],
            },
            Op::QueryExpr { type_id: 4, program: vec![0, 9, 9] },
            Op::AddOrderedIndex { type_id: 4, field_id: 2 },
            Op::FindRange { type_id: 4, field_id: 2, lo: vec![0], hi: vec![255, 255] },
            Op::Select { type_id: 4, program: vec![1, 2], limit: 10 },
            Op::QueryRows { type_id: 4, eq_preds: vec![(1, vec![9, 9])], program: vec![1], limit: 5, range_preds: vec![] },
            Op::QueryRows { type_id: 4, eq_preds: vec![], program: vec![1], limit: 0, range_preds: vec![(2, 1, vec![7, 0]), (2, 3, vec![9, 0])] },
            Op::Describe { type_id: 4 },
            Op::DropType { type_id: 4 },
            Op::Join { left_type: 4, right_type: 5, left_field: 1, right_field: 2, limit: 9 },
            Op::Aggregate { type_id: 4, program: vec![1], kind: 1, field_id: 3 },
            Op::SelectFields { type_id: 4, program: vec![1], fields: vec![1, 3], limit: 5 },
            Op::GroupAggregate { type_id: 4, program: vec![1], group_field: 1, kind: 1, agg_field: 3 },
            Op::SelectSorted { type_id: 4, program: vec![1], sort_field: 3, desc: true, offset: 2, limit: 5 },
            Op::AddCompositeIndex { type_id: 4, fields: vec![1, 3] },
            Op::DropIndex { type_id: 4, fields: vec![1] },
            Op::DropIndex { type_id: 4, fields: vec![1, 3] },
            Op::DropField { type_id: 4, field_id: 2 },
            Op::RenameField { type_id: 4, field_id: 2, name: "renamed".into() },
            Op::AddBalanceGuard { type_id: 4, field_id: 2 },
            Op::SeqAppend { payload: vec![1, 2, 3, 9] },
            Op::SeqRead { from: 7, limit: 0 },
            Op::XshardApply {
                seq: 5,
                ops: vec![Op::Delete { type_id: 1, id: ObjectId::from_u128(9) }],
            },
            Op::SeqAppendOnce { key: vec![7, 7], payload: vec![1, 2, 3] },
            Op::UpdateSet {
                type_id: 4,
                id: ObjectId::from_u128(8),
                sets: vec![(1, vec![9, 0, 0, 0]), (3, vec![1])],
            },
            Op::XshardDecide {
                seq: 6,
                ops: vec![Op::Delete { type_id: 1, id: ObjectId::from_u128(2) }],
            },
            Op::XshardCommit {
                seq: 6,
                ops: vec![Op::Delete { type_id: 1, id: ObjectId::from_u128(2) }],
                commit: true,
            },
            Op::FindByComposite { type_id: 4, fields: vec![1, 3], values: vec![vec![9], vec![8, 8]] },
        ];
        for op in ops {
            let enc = op.encode();
            let dec = Op::decode(&enc).expect("decode");
            assert_eq!(op, dec);
            assert_eq!(op.kind(), enc[0]);
        }
    }

    #[test]
    fn external_source_ops_wire_round_trip() {
        for op in [
            Op::CreateExternalSource {
                name: "feed".into(), type_def: vec![1,2,3], url: "http://h/p".into(),
                format: 0, key_field_id: 2, auth_kind: 1,
                auth_a: "TOKEN_ENV".into(), auth_b: String::new(),
                mapping: vec![(1,"id".into()), (2,"k".into())],
            },
            Op::DropExternalSource { name: "feed".into() },
            Op::RefreshExternalSource { name: "feed".into() },
        ] {
            let back = Op::decode(&op.encode()).expect("decode");
            assert_eq!(back.encode(), op.encode(), "round-trip mismatch");
            assert!(op.is_mutating());
        }
    }

    #[test]
    fn opresult_roundtrip_all_variants() {
        for r in [
            OpResult::Ok,
            OpResult::Got(vec![1, 2, 3, 250]),
            OpResult::Got(vec![]),
            OpResult::Exists,
            OpResult::NotFound,
            OpResult::TypeCreated(77),
            OpResult::SchemaError("nope".into()),
            OpResult::Constraint("UNIQUE x".into()),
            OpResult::Unavailable,
            OpResult::Unauthorized,
        ] {
            assert_eq!(OpResult::decode(&r.encode()), Some(r));
        }
        assert_eq!(OpResult::decode(&[9]), None);
        assert_eq!(OpResult::decode(&[]), None);
    }

    #[test]
    fn object_id_u128_roundtrip() {
        for v in [0u128, 1, u128::MAX, 0x1234_5678_9ABC] {
            assert_eq!(ObjectId::from_u128(v).as_u128(), v);
        }
    }

    #[test]
    fn crc32c_known_vectors() {
        // CRC-32C check value for ASCII "123456789" is 0xE3069283.
        assert_eq!(codec::crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(codec::crc32c(b""), 0x0000_0000);
        // Bit-flip changes the CRC.
        assert_ne!(codec::crc32c(b"abc"), codec::crc32c(b"abd"));
    }

    #[test]
    fn rng_is_deterministic_per_seed() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        let mut c = Rng::new(43);
        let sa: Vec<u64> = (0..16).map(|_| a.next_u64()).collect();
        let sb: Vec<u64> = (0..16).map(|_| b.next_u64()).collect();
        let sc: Vec<u64> = (0..16).map(|_| c.next_u64()).collect();
        assert_eq!(sa, sb, "same seed must reproduce");
        assert_ne!(sa, sc, "different seed must diverge");
    }

    #[test]
    fn decode_rejects_truncated() {
        assert!(Op::decode(&[3, 4, 0, 0]).is_none());
        assert!(Op::decode(&[]).is_none());
        assert!(Op::decode(&[99]).is_none());
    }
}
