//! kessel-proto: wire/log types, little-endian codec primitives, and a
//! deterministic PRNG. Dependency-free on purpose — determinism is a feature.

#![forbid(unsafe_code)]

use std::sync::Arc;

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

/// SP-PG-SQL-OUTER-JOIN: the join flavour carried by `Op::Join`. `Inner` is the
/// default (equi-join: only left rows with a matching right row are emitted) and
/// is wire-identical to the pre-arc bare/filtered join. `Left` is a LEFT [OUTER]
/// JOIN: EVERY left row is emitted; a left row with no matching right row is
/// emitted once with all right (`b.*`) fields NULL. The wire byte is appended
/// only when non-`Inner`, so an older frame (or any inner join) decodes to
/// `Inner` — byte-identical to before.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum JoinType {
    #[default]
    Inner,
    Left,
}

impl JoinType {
    /// Stable wire tag (only emitted when non-`Inner`). RIGHT/FULL are named
    /// follow-ups; reserve their tags here so a future arc stays compatible.
    pub fn wire_tag(self) -> u8 {
        match self {
            JoinType::Inner => 0,
            JoinType::Left => 1,
        }
    }
    pub fn from_wire_tag(t: u8) -> Option<Self> {
        match t {
            0 => Some(JoinType::Inner),
            1 => Some(JoinType::Left),
            _ => None,
        }
    }
}

/// SP-PG-SQL-MULTI-JOIN: one additional `JOIN <table> ON <combined-col> =
/// <table>.<col>` segment chained after the base binary join. Each step
/// extends the running combined `(a ++ b ++ …)` row set by INNER equi-joining
/// it against `right_type` on `left_combined_field == right_field`.
///   - `right_type` is the next table's TypeId.
///   - `left_combined_field` is a field id in the RUNNING combined schema
///     built so far (`0..combined_width`), i.e. the `<table>.<col>` column the
///     ON's left side resolves to (it may reference ANY already-joined table).
///   - `right_field` is the join column's field id in `right_type`.
/// V1 is INNER equi-join only (matching the base join's INNER path); the
/// combined schema grows by `right_type`'s fields each step, renamed
/// `<right_table>.<col>` with fresh sequential combined field ids.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinStep {
    pub right_type: TypeId,
    pub left_combined_field: u16,
    pub right_field: u16,
}

/// SP-PG-SQL-MULTI-JOIN: distinct marker byte for the chained extra-join block.
/// It shares the post-page-block position with the ga block (marker `1`); using
/// a DIFFERENT marker (`2`) lets the decoder pick the right block WITHOUT a
/// presence anchor — so a ga-only frame stays BYTE-IDENTICAL to a pre-arc frame
/// (no anchor byte added) while an extra-joins frame is self-identifying. V1
/// never emits both (multi-join + group-aggregate is a named follow-up).
const EXTRA_JOINS_MARKER: u8 = 2;

/// SP-PG-SQL-MULTI-JOIN: marker-guarded encode of the chained extra-join list.
/// Empty ⇒ writes NOTHING (a 2-table join is byte-identical to a pre-arc
/// frame). Non-empty ⇒ `[u8 2 marker][u16 count][ (u32 right_type)(u16
/// left_combined_field)(u16 right_field) ]*`.
fn encode_extra_joins(b: &mut Vec<u8>, steps: &[JoinStep]) {
    if steps.is_empty() {
        return;
    }
    b.push(EXTRA_JOINS_MARKER);
    b.extend_from_slice(&(steps.len() as u16).to_le_bytes());
    for s in steps {
        codec::put_u32(b, s.right_type);
        b.extend_from_slice(&s.left_combined_field.to_le_bytes());
        b.extend_from_slice(&s.right_field.to_le_bytes());
    }
}

/// SP-PG-SQL-MULTI-JOIN: read `count` chained extra-join steps. The caller has
/// already CONSUMED the `EXTRA_JOINS_MARKER` byte (it peeked to distinguish the
/// extra-joins block from the ga block). count==0 is malformed ⇒ `Err`.
fn read_extra_joins_body(c: &mut codec::Cursor) -> Result<Vec<JoinStep>, ()> {
    let n = c.u16().ok_or(())? as usize;
    if n == 0 {
        return Err(());
    }
    let mut steps = Vec::with_capacity(n);
    for _ in 0..n {
        let right_type = c.u32().ok_or(())?;
        let left_combined_field = c.u16().ok_or(())?;
        let right_field = c.u16().ok_or(())?;
        steps.push(JoinStep { right_type, left_combined_field, right_field });
    }
    Ok(steps)
}

/// SP-PG-SQL-JOIN-AGG: a `GROUP BY` + aggregate spec over the COMBINED join
/// `(a ++ b)` schema. `group_field` is the combined-schema field id to group by;
/// `aggregates` is `Vec<(kind, field_id)>` with the canonical aggregate kind
/// codes (0 COUNT / 1 SUM / 2 MIN / 3 MAX / 4 AVG, mirroring `Op::Aggregate`).
/// For `COUNT(*)` the field id is the sentinel `COUNT_STAR_FIELD` (count rows);
/// `COUNT(col)` carries the real combined field id (count non-NULL values —
/// PostgreSQL semantics, so a LEFT-join unmatched right column counts 0). Both
/// ids are references into the combined `(a ++ b)` layout the engine builds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinGroupAgg {
    pub group_field: u16,
    pub aggregates: Vec<(u8, u16)>,
    /// SP-PG-SQL-HAVING: optional post-aggregation group filter. `None`
    /// (default) ⇒ every group is emitted (byte-identical to a pre-HAVING
    /// frame). When `Some`, a group is dropped unless `having.keep(results)`.
    pub having: Option<HavingPred>,
}

/// Sentinel `field_id` paired with kind 0 (COUNT) ⇒ `COUNT(*)` (count every
/// combined row), distinct from `COUNT(col)` which counts non-NULL values.
pub const COUNT_STAR_FIELD: u16 = u16::MAX;

/// SP-PG-SQL-HAVING: a `HAVING <agg> <cmp> <literal>` group filter applied
/// AFTER aggregation, BEFORE order_by/limit/offset paging. `agg_index` selects
/// which aggregate in the op's aggregate output sequence to compare (for the
/// single-aggregate `Op::GroupAggregate` it is always 0; for the multi /
/// join-group-aggregate it indexes into `aggregates`). `op` is the comparison
/// (0 `>` / 1 `>=` / 2 `<` / 3 `<=` / 4 `=` / 5 `<>`), and `value` is the
/// right-hand-side integer/numeric literal as an i128 (the same i128 the
/// aggregate result is computed as). A group is KEPT iff
/// `agg_result(agg_index) <op> value`.
///
/// The HAVING predicate is a PURE function of the already-deterministic
/// per-group aggregate output, so applying it on the single deterministic
/// apply thread keeps the result a pure function of the input rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HavingPred {
    pub agg_index: u16,
    pub op: u8,
    pub value: i128,
}

impl HavingPred {
    /// True iff a group whose aggregate output sequence is `agg_results`
    /// should be KEPT. `agg_index` out of range ⇒ keep (defensive; the SQL
    /// layer never emits an out-of-range index — it is validated at compile).
    pub fn keep(&self, agg_results: &[i128]) -> bool {
        let lhs = match agg_results.get(self.agg_index as usize) {
            Some(v) => *v,
            None => return true,
        };
        match self.op {
            0 => lhs > self.value,
            1 => lhs >= self.value,
            2 => lhs < self.value,
            3 => lhs <= self.value,
            4 => lhs == self.value,
            5 => lhs != self.value,
            _ => true,
        }
    }

    /// Canonical comparison wire code for an SQL comparison operator string.
    /// `> >= < <= = <> !=` → `0 1 2 3 4 5 5`. `None` for any other operator.
    pub fn op_code(cmp: &str) -> Option<u8> {
        match cmp {
            ">" => Some(0),
            ">=" => Some(1),
            "<" => Some(2),
            "<=" => Some(3),
            "=" => Some(4),
            "<>" | "!=" => Some(5),
            _ => None,
        }
    }
}

/// SP-PG-SQL-HAVING: marker-guarded encode of an optional HAVING block.
/// `None` ⇒ writes NOTHING (byte-identical to a pre-HAVING frame). `Some` ⇒
/// `[u8 1][u16 agg_index][u8 op][16B i128 LE value]`. A non-1 marker on decode
/// is a forward-incompatible op (rejected), mirroring the other marker blocks.
fn encode_having(b: &mut Vec<u8>, having: &Option<HavingPred>) {
    if let Some(h) = having {
        b.push(1u8); // having-block marker
        b.extend_from_slice(&h.agg_index.to_le_bytes());
        b.push(h.op);
        b.extend_from_slice(&h.value.to_le_bytes());
    }
}

/// SP-PG-SQL-HAVING: decode the optional trailing HAVING block. Absent (no
/// remaining bytes) ⇒ `None`. Marker 1 ⇒ read the predicate. Marker 0 ⇒
/// SP-PG-SQL-GROUP-SORT-LIMIT "no-HAVING anchor" (a group-sort block follows;
/// the anchor is consumed so the sort decode can read its own marker) ⇒ `None`.
/// Any other marker is a forward-incompatible op ⇒ `Err` (surfaced as a decode
/// failure, never silently mis-applied). Returns `Ok(None)`/`Ok(Some(_))`.
fn decode_having(c: &mut codec::Cursor) -> Result<Option<HavingPred>, ()> {
    if c.remaining() == 0 {
        return Ok(None);
    }
    match c.u8() {
        Some(1) => {
            let agg_index = c.u16().ok_or(())?;
            let op = c.u8().ok_or(())?;
            let value = c.u128().ok_or(())? as i128;
            Ok(Some(HavingPred { agg_index, op, value }))
        }
        // SP-PG-SQL-GROUP-SORT-LIMIT no-HAVING anchor: consumed, no predicate.
        Some(0) => Ok(None),
        _ => Err(()),
    }
}

/// SP-PG-SQL-GROUP-SORT-LIMIT: write the combined HAVING + group-sort trailer
/// so the two compose without ambiguity while preserving byte-identity for
/// every pre-arc frame:
///   - no HAVING, no sort ⇒ write NOTHING (pre-arc identical).
///   - HAVING, no sort     ⇒ the existing `encode_having` block ONLY (a
///     pre-group-sort HAVING-only frame is byte-identical).
///   - sort present        ⇒ a HAVING-presence anchor (`encode_having` writes
///     `[1][..]` when Some; we write a single `0` when None) FOLLOWED by the
///     group-sort block. The `0` anchor lets `decode_having` consume the
///     no-HAVING case and hand off to `decode_group_sort`.
fn encode_group_trailer(b: &mut Vec<u8>, having: &Option<HavingPred>, sort: &Option<GroupSort>) {
    match (having, sort) {
        (_, None) => encode_having(b, having),
        (Some(_), Some(_)) => {
            encode_having(b, having);
            encode_group_sort(b, sort);
        }
        (None, Some(_)) => {
            b.push(0u8); // no-HAVING anchor so the sort block has a fixed offset
            encode_group_sort(b, sort);
        }
    }
}

/// SP-PG-SQL-GROUP-SORT-LIMIT: a post-aggregation `ORDER BY … [ASC|DESC]
/// [LIMIT n] [OFFSET m]` over the per-group result of a PLAIN (non-JOIN)
/// `GROUP BY`. Applied AFTER aggregation AND AFTER any HAVING filter, on the
/// single deterministic apply thread, over the already-deterministic per-group
/// `(key, [agg results])` sequence — so it stays a pure function of the input
/// rows.
///
/// `target` selects WHAT to sort by:
///   - `GroupSortTarget::Key` ⇒ sort by the raw group-key bytes (`ORDER BY g`
///     / `ORDER BY 1`).
///   - `GroupSortTarget::Agg(i)` ⇒ sort by the i128 value of the i-th aggregate
///     in the op's aggregate output sequence (`ORDER BY n` / `ORDER BY 2` /
///     `ORDER BY COUNT(*)`). For the single-aggregate `Op::GroupAggregate` `i`
///     is always 0.
/// `desc` reverses the comparison. Ties are ALWAYS broken by ascending group
/// key (the pre-arc emission order), giving a TOTAL deterministic order.
/// `limit`/`offset`: `None` ⇒ unbounded / 0; applied AFTER the sort.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupSortTarget {
    /// Sort by the raw group-key bytes (lexicographic over the fixed-width key).
    Key,
    /// Sort by the i128 value of aggregate slot `0`-based index.
    Agg(u16),
}

/// SP-PG-SQL-GROUP-SORT-LIMIT: see `GroupSortTarget`. Carried (optionally) by
/// `Op::GroupAggregate` / `Op::GroupAggregateMulti`. `None` ⇒ pre-arc behaviour
/// (groups emitted in ascending key order, unbounded — byte-identical frame).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupSort {
    pub target: GroupSortTarget,
    pub desc: bool,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// SP-PG-SQL-GROUP-SORT-LIMIT: marker-guarded encode of an optional group
/// sort/page block. `None` ⇒ writes NOTHING (byte-identical to a pre-arc
/// frame). `Some` ⇒ `[u8 1 marker][u8 target_tag][u16 agg_index][u8 desc]
/// [u8 has_limit][?u64 limit][u8 has_offset][?u64 offset]`. `target_tag`
/// 0 = Key (agg_index written as 0, ignored), 1 = Agg(agg_index). A non-1
/// marker on decode is a forward-incompatible op (rejected), mirroring
/// `encode_having`/`decode_having`. This block is positioned AFTER the
/// HAVING block so the two compose without ambiguity.
fn encode_group_sort(b: &mut Vec<u8>, sort: &Option<GroupSort>) {
    if let Some(s) = sort {
        b.push(1u8); // group-sort-block marker
        let (tag, agg_index) = match s.target {
            GroupSortTarget::Key => (0u8, 0u16),
            GroupSortTarget::Agg(i) => (1u8, i),
        };
        b.push(tag);
        b.extend_from_slice(&agg_index.to_le_bytes());
        b.push(s.desc as u8);
        match s.limit {
            Some(n) => {
                b.push(1u8);
                b.extend_from_slice(&n.to_le_bytes());
            }
            None => b.push(0u8),
        }
        match s.offset {
            Some(n) => {
                b.push(1u8);
                b.extend_from_slice(&n.to_le_bytes());
            }
            None => b.push(0u8),
        }
    }
}

/// SP-PG-SQL-GROUP-SORT-LIMIT: decode the optional trailing group-sort block.
/// Absent (no remaining bytes) ⇒ `Ok(None)`. Marker 1 ⇒ read the block. A
/// non-1 marker, or a target tag other than 0/1, is a forward-incompatible op
/// ⇒ `Err` (surfaced as a decode failure, never silently mis-applied).
fn decode_group_sort(c: &mut codec::Cursor) -> Result<Option<GroupSort>, ()> {
    if c.remaining() == 0 {
        return Ok(None);
    }
    match c.u8() {
        Some(1) => {
            let tag = c.u8().ok_or(())?;
            let agg_index = c.u16().ok_or(())?;
            let desc = c.u8().ok_or(())? != 0;
            let target = match tag {
                0 => GroupSortTarget::Key,
                1 => GroupSortTarget::Agg(agg_index),
                _ => return Err(()),
            };
            let limit = if c.u8().ok_or(())? != 0 {
                Some(c.u64().ok_or(())?)
            } else {
                None
            };
            let offset = if c.u8().ok_or(())? != 0 {
                Some(c.u64().ok_or(())?)
            } else {
                None
            };
            Ok(Some(GroupSort { target, desc, limit, offset }))
        }
        _ => Err(()),
    }
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
    /// `range_preds` (SP-Analytic-Plan) mirror the `Op::QueryRows` field:
    /// `(field_id, op, value)` half-range hints (`op` 0=`>` 1=`>=` 2=`<`
    /// 3=`<=`) on order-indexed fields. When non-empty, the SM narrows
    /// the scan via the existing ordered-index machinery BEFORE the
    /// row-by-row program filter. `program` (the full WHERE) still
    /// verifies every candidate, so the aggregate result is identical
    /// regardless of the candidate set — the indexes only accelerate.
    /// `range_preds` is encoded *after* `field_id` so an older frame
    /// (no range hints) decodes to an empty list and behaves exactly
    /// as before (wire-compatible).
    /// Result returned as a 16-byte little-endian i128 in `Got`.
    Aggregate {
        type_id: TypeId,
        program: Vec<u8>,
        kind: u8,
        field_id: u16,
        range_preds: Vec<(u16, u8, Vec<u8>)>,
    },
    /// Projection (Sub-project 21): like `Select` but each returned row is
    /// only the concatenated bytes of `fields` (in order), not the whole
    /// record. Result = `[u32 rowlen][row]*`. Read-only & deterministic.
    SelectFields { type_id: TypeId, program: Vec<u8>, fields: Vec<u16>, limit: u32 },
    /// GROUP BY aggregate (Sub-project 22): over rows matching `program`,
    /// group by `group_field`'s value and compute `kind` (0 COUNT / 1 SUM /
    /// 2 MIN / 3 MAX) of `agg_field` per group. Result =
    /// `[u32 ngroups]` then per group `[u32 keylen][key][16B i128 LE]`,
    /// groups in ascending key order (deterministic). Read-only.
    /// `range_preds` (SP-Analytic-Plan) mirror `Op::QueryRows`: when
    /// non-empty, the SM narrows the scan via the existing ordered-
    /// index machinery BEFORE the row-by-row program filter (and the
    /// group fold). Wire-back-compat via length-prefixed conditional
    /// trailing encode (matches the `Op::Aggregate` shape).
    GroupAggregate {
        type_id: TypeId,
        program: Vec<u8>,
        group_field: u16,
        kind: u8,
        agg_field: u16,
        range_preds: Vec<(u16, u8, Vec<u8>)>,
        /// SP-PG-SQL-HAVING: optional post-aggregation group filter (agg_index
        /// is always 0 here — there is exactly one aggregate). `None` ⇒
        /// every group emitted (byte-identical to a pre-HAVING frame).
        having: Option<HavingPred>,
        /// SP-PG-SQL-GROUP-SORT-LIMIT: optional `ORDER BY … [LIMIT][OFFSET]`
        /// over the per-group result. Applied AFTER aggregation + HAVING.
        /// `None` ⇒ ascending-key order, unbounded (byte-identical to a
        /// pre-arc frame). Sort target `Agg(i)` MUST have `i == 0` here (one
        /// aggregate); `Key` sorts by group key.
        sort: Option<GroupSort>,
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
        type_def: Vec<u8>,
        url: String,
        format: u8,
        key_field_id: u16,
        auth_kind: u8,
        auth_a: String,
        auth_b: String,
        mapping: Vec<(u16, String)>,
        /// JSON path to the rows array (external-sources pagination
        /// follow-on; `None` = slice-1 default / whole body).
        rows_path: Option<String>,
        /// Pagination recipe carrier `(tag, a, b)`: tag 1 =
        /// NextUrlJson(a=json path), 2 = NextLink (a,b empty), 3 =
        /// CursorJson{a=path, b=param}. `None` = slice-1 default.
        /// MUST match `kessel_catalog::PaginationRecipe`'s wire tags; adding a variant there requires a new tag here AND a WAL protocol-version bump.
        pagination: Option<(u8, String, String)>,
        /// Object-store extras `(provider, account, region, endpoint)`.
        /// provider 1=S3 / 2=Azure; strings may be empty. `None` =
        /// not an object-store source / older frame (tolerant decode —
        /// absent ⇒ None, never a failure).
        objstore: Option<(u8, String, String, String)>,
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
    ///
    /// SP-PG-SQL-JOIN-WHERE: `filter` is an OPTIONAL `kessel-expr` predicate
    /// program over the COMBINED join schema (left fields `<lt>.<col>` then
    /// right fields `<rt>.<col>`, field ids `0..nL+nR`). When non-empty, each
    /// combined row is kept only if `eval(filter, combined_type, combined_rec)`
    /// is true. Empty (the default) ⇒ no filter ⇒ byte-identical to a bare
    /// join. Encoded only when non-empty, so an older bare-join frame is a
    /// valid prefix that decodes to an empty filter.
    Join {
        left_type: TypeId,
        right_type: TypeId,
        left_field: u16,
        right_field: u16,
        limit: u32,
        filter: Vec<u8>,
        /// SP-PG-SQL-OUTER-JOIN: `Inner` (default) or `Left`. Additive — see
        /// `JoinType`. Encoded only when non-`Inner`; an older / inner frame
        /// decodes to `Inner`, byte-identical to before.
        join_type: JoinType,
        /// SP-PG-SQL-JOIN-QUERY: optional `ORDER BY <combined-field id>`
        /// over the COMBINED `(a ++ b)` schema. `(field_id, desc)`. `None`
        /// (default) ⇒ no sort ⇒ rows emit in left-key/right-scan order
        /// (byte-identical to a pre-arc join). When `Some`, the engine
        /// stable-sorts the surviving combined rows by this field then
        /// applies `offset_n`/`limit_n`.
        order_by: Option<(u16, bool)>,
        /// SP-PG-SQL-JOIN-QUERY: optional POST-sort row cap. `None` ⇒
        /// unbounded. (The legacy `limit` field is a PRE-sort cap used by
        /// bare `JOIN … LIMIT n`; a sorted/paginated query sets `limit = 0`
        /// and paginates here so there is no double-cap.)
        limit_n: Option<u64>,
        /// SP-PG-SQL-JOIN-QUERY: optional POST-sort skip. `None` ⇒ 0.
        offset_n: Option<u64>,
        /// SP-PG-SQL-JOIN-AGG: optional `GROUP BY` + aggregate over the combined
        /// rows. `None` (default) ⇒ a plain join (emit/sort/paginate the combined
        /// rows — byte-identical to a pre-arc join). When `Some`, the engine
        /// groups the surviving combined rows by `group_field` and runs the
        /// aggregates per group, emitting the `[u32 ngroups][u32 keylen][key]
        /// [16B i128 × n_aggs]` group-aggregate result (the `Op::GroupAggregate
        /// Multi` shape) instead of the join row stream. `order_by`/`limit_n`/
        /// `offset_n` do NOT apply when grouping (V1; ORDER BY over the aggregate
        /// is the named follow-up SP-PG-SQL-JOIN-AGG-ORDERBY-AGG).
        group_aggregate: Option<JoinGroupAgg>,
        /// SP-PG-SQL-MULTI-JOIN: additional chained INNER equi-join steps after
        /// the base binary join. EMPTY (default) ⇒ a normal binary join ⇒
        /// BYTE-IDENTICAL `Op` frame to before this arc. When non-empty, the
        /// engine applies each step in order, INNER equi-joining the running
        /// combined `(a ++ b ++ …)` row set against the step's table on the ON
        /// columns, extending the combined schema each step. The `filter` /
        /// `order_by` / `limit_n` / `offset_n` then apply over the FINAL combined
        /// schema. (V1: chained extra joins do NOT combine with `group_aggregate`
        /// — that is a named follow-up.)
        extra_joins: Vec<JoinStep>,
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
    /// Plain SI conflict-checked commit (S2.3 / SP112). Carries the Tx's
    /// snapshot opnum + the deterministic-iteration write_set + the
    /// SM-assigned commit opnum. SM apply runs the
    /// `has_version_in_range(snapshot_opnum, commit_opnum-1)` check for
    /// each write_set key; on conflict, aborts; on no conflict, installs
    /// every write via put_versioned at commit_opnum. The verdict is a
    /// deterministic function of the log prefix (parent S2 design Decision
    /// 4). write_set is sorted by (type_id, object_id) at construction.
    /// Empty write_set => trivial commit (no-op apply). commit_opnum=0
    /// edge: the conflict check is skipped (no prior versions can exist).
    CommitTx {
        snapshot_opnum: u64,
        write_set: Vec<(u32, [u8; 16], Option<Vec<u8>>)>,
        commit_opnum: u64,
        /// SP113 / S2.4: SSI read-set tracking. Empty vec preserves SP112
        /// plain-SI behaviour (the SM apply arm's SSI inner branch is
        /// gated on `read_set.is_empty() == false`). Non-empty vec
        /// activates the Cahill SSI dangerous-structure detector. Sorted
        /// by (type_id, object_id) at construction for deterministic
        /// SM-side iteration order (mirrors write_set ordering).
        read_set: Vec<(u32, [u8; 16])>,
    },
    /// SP114 / S2.5: Advance the global low_water_mark for MVCC GC. The
    /// SM apply arm validates monotonicity (must be > current low_water_mark)
    /// and commit_opnum ceiling (must be <= current commit_opnum); on
    /// validation success, deletes every MVCC version with commit_opnum <
    /// low_water_mark via `mvcc::delete_versions_older_than`, prunes
    /// every pending_txs record with commit_opnum < low_water_mark via
    /// `ssi::prune_pending_txs_by_watermark`, and updates the SM's
    /// `low_water_mark` field. Outcome: `OpResult::WatermarkAdvanced` on
    /// success; `OpResult::WatermarkRejected` on validation failure.
    ///
    /// Heartbeat protocol: the value `low_water_mark` is computed externally
    /// (typically by the leader as `min(active_snapshot_opnum)` across the
    /// cluster) and submitted as this op. S2.5 ships the apply path; the
    /// heartbeat producer is OUT of scope (Decision 2). See
    /// `docs/superpowers/specs/2026-05-24-mvcc-si-s2-5-design.md`.
    AdvanceWatermark { low_water_mark: u64 },

    /// SP-Analytic-Plan-MULTI: multi-aggregate single-scan GROUP BY.
    /// Collapses N×`Op::GroupAggregate` (each doing its own full-narrowed
    /// scan) into ONE scan that folds N aggregates per row. Closes the
    /// SP-Analytic-Plan T4 residual Q1 gap (the WHERE-narrowing prong
    /// shipped V1; this is the multi-aggregate-fold prong V2).
    ///
    /// `aggregates` = `Vec<(kind, field_id)>`. `kind` mirrors the existing
    /// `Op::Aggregate` codes: 0=COUNT, 1=SUM, 2=MIN, 3=MAX, 4=AVG. For
    /// COUNT the `field_id` is ignored (convention: 0). Must be non-empty
    /// (single-aggregate callers use the existing `Op::GroupAggregate`).
    ///
    /// `range_preds` (mirrors `Op::GroupAggregate`): half-range hints on
    /// order-indexed fields; the SM narrows the scan via the existing
    /// ordered-index machinery BEFORE the row-by-row program filter +
    /// group fold. `program` still verifies every candidate, so the
    /// aggregate result is identical regardless of the candidate set —
    /// the indexes only accelerate.
    ///
    /// Result encoding: `[u32 ngroups]` then per group
    /// `[u32 keylen][key][n_aggs × 16B i128 LE]`, groups in ascending
    /// key order (deterministic, mirrors `Op::GroupAggregate`'s shape
    /// but with N per-group values instead of 1; `n_aggs` is implicit —
    /// the caller knows it from the request).
    GroupAggregateMulti {
        type_id: TypeId,
        program: Vec<u8>,
        group_field: u16,
        aggregates: Vec<(u8, u16)>,
        range_preds: Vec<(u16, u8, Vec<u8>)>,
        /// SP-PG-SQL-HAVING: optional post-aggregation group filter. `None` ⇒
        /// every group emitted (byte-identical to a pre-HAVING frame).
        having: Option<HavingPred>,
        /// SP-PG-SQL-GROUP-SORT-LIMIT: optional `ORDER BY … [LIMIT][OFFSET]`
        /// over the per-group result. Applied AFTER aggregation + HAVING.
        /// `None` ⇒ ascending-key order, unbounded (byte-identical to a
        /// pre-arc frame). Sort target `Agg(i)` indexes into `aggregates`.
        sort: Option<GroupSort>,
    },

    /// SP123 / S2.X: per-replica active-snapshot report — closes the
    /// SP115-shipped honest caveat that `active_snapshots` is per-replica
    /// local. Each replica periodically submits this op via VSR carrying
    /// `(self.replica_id, sm.min_active_snapshot())`. Since VSR replicates
    /// the log, all replicas observe all reports + deterministically
    /// compute the GLOBAL minimum across their `replica_min_snapshots`
    /// BTreeMap.
    ///
    /// The heartbeat producer THEN submits `Op::AdvanceWatermark` with
    /// `low_water_mark <= sm.global_min_active_snapshot()` instead of just
    /// `sm.min_active_snapshot()` — preventing the watermark from
    /// advancing past a snapshot held by a DIFFERENT replica.
    ///
    /// Monotonicity per replica: each replica's claimed min is
    /// monotonic-strict (a replica can only RELEASE earlier snapshots,
    /// never re-acquire them with a smaller min); the SM apply arm
    /// rejects a report whose claimed min is < the replica's previously
    /// reported value.
    ///
    /// Outcome: `OpResult::ActiveSnapshotReported { replica_id, accepted_min }`
    /// on accept; `OpResult::ActiveSnapshotRejected { reason }` on
    /// validation failure.
    ReportActiveSnapshot { replica_id: u32, min_active_snapshot: u64 },
}

/// SP114 / S2.5: Why an `Op::AdvanceWatermark` was rejected by the
/// SM apply arm. Per S2.5 design Decision 5: strict monotonicity +
/// commit_opnum ceiling. Both rejection variants are deterministic
/// pure functions of `(proposed watermark, prior SM state)`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WatermarkRejection {
    /// Proposed watermark is <= current watermark (strict monotonicity
    /// violation). Heartbeat producer error — duplicate or out-of-order op.
    /// Encoded at sub-tag 0.
    NotMonotonic { proposed: u64, current: u64 },
    /// Proposed watermark exceeds the SM's current commit_opnum
    /// (would reclaim versions that have not been committed).
    /// Heartbeat producer bug. Encoded at sub-tag 1.
    AboveCommitCeiling { proposed: u64, current_commit: u64 },
}

/// Why an `Op::CommitTx` apply path aborted. Carried inside
/// `OpResult::TxAborted`. SP112 T2 adds this enum alongside the
/// `TxCommitted` / `TxAborted` OpResult variants.
///
/// SP112 T2-DECIDED CHOICE (OpResult shape) — Strategy (a) from the
/// design's "Add new OpResult variants" option. Rationale:
///   - Strategy (a): typed, no string-parsing on the caller side; the
///     `conflicting_key` / underlying I/O `kind` survive the wire trip
///     without being losslessly re-extracted from a payload bag.
///   - Strategy (b): "encode via existing variant + a payload byte
///     sequence" would have hidden the typed result inside e.g.
///     `OpResult::Got(bytes)`, forcing every consumer to know the
///     bag's shape — a footgun and a maintenance hazard.
/// Implementation cost: ~12 lines of encode/decode in this file +
/// no callsite churn (all existing `match` arms continue to work via
/// the SP1-existing `_` arms or are non-exhaustive-flagged).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AbortReason {
    /// `snapshot_opnum > commit_opnum` — malformed input on the wire.
    /// Mirrors `TxError::SnapshotOutOfRange` from kessel-storage::tx.
    SnapshotOutOfRange,
    /// `has_version_in_range(snapshot, commit-1)` returned `true` for
    /// `(type_id, object_id)` — first-committer-wins; this Tx aborts.
    WriteWriteConflict { type_id: u32, object_id: [u8; 16] },
    /// `put_versioned` failed during the install phase. `kind` is
    /// `std::io::ErrorKind as i32` so the variant stays `Clone + Eq`.
    StorageIo { kind: i32 },
    /// SP113 / S2.4: The committing Tx was the pivot or outer node of
    /// an SSI dangerous structure (two consecutive rw-antidependency
    /// edges in the rw-edge graph). Aborted per Cahill SSI to preserve
    /// serializability. Replay with a fresh snapshot. `other_commit_opnum`
    /// is the commit_opnum of the other Tx in the rw-edge chain (for
    /// debugging + observability; does NOT affect the verdict).
    DangerousStructure { other_commit_opnum: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpResult {
    Ok,
    /// SP-Perf-A T6 Fix B: payload is `Arc<[u8]>` so the in-process read
    /// fast path (`StateMachine::read_only_op` → `OpResult::Got`) can return
    /// a refcount-bump clone of the storage-resident bytes instead of
    /// allocating + memcpy'ing a fresh Vec on every read. The wire format
    /// is unchanged: `encode()` writes the bytes via `as_ref()`, and
    /// `decode()` allocates a Vec then wraps it once via `Arc::from(vec)`.
    /// Subsequent in-process clones of `OpResult::Got` are then atomic
    /// refcount bumps, not heap allocations.
    Got(Arc<[u8]>),
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
    /// SP112 / S2.3: `Op::CommitTx` succeeded; every write installed at
    /// `commit_opnum` via `put_versioned`. Echoes the commit_opnum for
    /// audit. The verdict is a deterministic function of the log prefix.
    TxCommitted { commit_opnum: u64 },
    /// SP112 / S2.3: `Op::CommitTx` aborted; reason explains why. The
    /// caller (Tx wrapper or SQL driver) should retry with a fresher
    /// snapshot for `WriteWriteConflict`, or surface the error for
    /// `SnapshotOutOfRange` / `StorageIo`.
    TxAborted { reason: AbortReason },
    /// SP114 / S2.5: `Op::AdvanceWatermark` accepted by the SM apply arm.
    /// Surfaces the new watermark + observability counts.
    WatermarkAdvanced {
        new_low_water_mark: u64,
        versions_deleted: usize,
        pending_txs_evicted: usize,
    },
    /// SP114 / S2.5: `Op::AdvanceWatermark` rejected by SM validation.
    WatermarkRejected { reason: WatermarkRejection },
    /// SP123 / S2.X: `Op::ReportActiveSnapshot` accepted; carries the
    /// (replica_id, accepted_min) for observability + replay validation.
    ActiveSnapshotReported { replica_id: u32, accepted_min: u64 },
    /// SP123 / S2.X: `Op::ReportActiveSnapshot` rejected (non-monotonic
    /// per-replica). Carries the previously-reported min for diagnostics.
    ActiveSnapshotRejected { replica_id: u32, previous_min: u64, proposed: u64 },
    /// SP-PG-SERIAL-RETURNING: `Op::Create` succeeded AND the engine
    /// deterministically ASSIGNED the row id from a per-type SERIAL
    /// sequence (the caller passed the `SERIAL_SENTINEL` id on a
    /// `serial_pk` type). Carries the assigned 128-bit id so the gateway
    /// can render an `INSERT … RETURNING id` DataRow. A plain explicit-id
    /// Create still returns `OpResult::Ok` (byte-identical to before), so
    /// this variant fires ONLY on the autoincrement path.
    Created { id: u128 },
    /// SP-PG-RETURNING-MULTIROW-STAR: an `Op::Txn` whose inner ops were
    /// ALL `Op::Create`s that each returned `OpResult::Created { id }`
    /// (the multi-row autoincrement `INSERT … VALUES (…),(…) RETURNING`
    /// shape SQLAlchemy emits by default with `use_insertmanyvalues`).
    /// Carries the assigned ids in insertion order so the gateway emits
    /// one DataRow per row. A Txn that is NOT all-Create-returning-Created
    /// still returns `OpResult::Ok` (byte-identical to before), so this
    /// variant fires ONLY on the multi-row autoincrement INSERT path. The
    /// ids are a deterministic function of the committed log prefix (the
    /// SERIAL counter writes), so this carries no clock/RNG state.
    CreatedMany { ids: Vec<u128> },
}

impl OpResult {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        match self {
            OpResult::Ok => b.push(0),
            OpResult::Got(v) => {
                b.push(1);
                // SP-Perf-A T6 Fix B: bytes via Arc<[u8]> auto-deref; wire
                // encoding identical to the prior Vec<u8> shape.
                codec::put_bytes(&mut b, v.as_ref());
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
            // SP112 T2: TxCommitted/TxAborted tagged 9 and 10. AbortReason
            // is sub-tagged inside the TxAborted payload (0=SnapshotOOR,
            // 1=WriteWriteConflict{type,obj16}, 2=StorageIo{kind:i32}).
            OpResult::TxCommitted { commit_opnum } => {
                b.push(9);
                codec::put_u64(&mut b, *commit_opnum);
            }
            OpResult::TxAborted { reason } => {
                b.push(10);
                match reason {
                    AbortReason::SnapshotOutOfRange => b.push(0),
                    AbortReason::WriteWriteConflict { type_id, object_id } => {
                        b.push(1);
                        codec::put_u32(&mut b, *type_id);
                        b.extend_from_slice(object_id);
                    }
                    AbortReason::StorageIo { kind } => {
                        b.push(2);
                        // i32 LE via u32 transmute-bytes (Clone+Eq friendly).
                        codec::put_u32(&mut b, *kind as u32);
                    }
                    // SP113 / S2.4: DangerousStructure at sub-tag 3.
                    AbortReason::DangerousStructure { other_commit_opnum } => {
                        b.push(3);
                        codec::put_u64(&mut b, *other_commit_opnum);
                    }
                }
            }
            // SP114 / S2.5: WatermarkAdvanced at tag 11.
            // wire: [u64 new_low_water_mark][u64 versions_deleted][u64 pending_txs_evicted]
            // (usize encoded as u64 LE for platform-independence)
            OpResult::WatermarkAdvanced { new_low_water_mark, versions_deleted, pending_txs_evicted } => {
                b.push(11);
                codec::put_u64(&mut b, *new_low_water_mark);
                codec::put_u64(&mut b, *versions_deleted as u64);
                codec::put_u64(&mut b, *pending_txs_evicted as u64);
            }
            // SP114 / S2.5: WatermarkRejected at tag 12.
            // wire: [u8 sub-tag] [payload per variant]
            //   sub-tag 0 = NotMonotonic:       [u64 proposed][u64 current]
            //   sub-tag 1 = AboveCommitCeiling: [u64 proposed][u64 current_commit]
            OpResult::WatermarkRejected { reason } => {
                b.push(12);
                match reason {
                    WatermarkRejection::NotMonotonic { proposed, current } => {
                        b.push(0);
                        codec::put_u64(&mut b, *proposed);
                        codec::put_u64(&mut b, *current);
                    }
                    WatermarkRejection::AboveCommitCeiling { proposed, current_commit } => {
                        b.push(1);
                        codec::put_u64(&mut b, *proposed);
                        codec::put_u64(&mut b, *current_commit);
                    }
                }
            }
            // SP123 / S2.X: ActiveSnapshotReported (13) wire encode.
            //   wire: [u32 replica_id (LE)] [u64 accepted_min (LE)]
            OpResult::ActiveSnapshotReported { replica_id, accepted_min } => {
                b.push(13);
                codec::put_u32(&mut b, *replica_id);
                codec::put_u64(&mut b, *accepted_min);
            }
            // SP123 / S2.X: ActiveSnapshotRejected (14) wire encode.
            //   wire: [u32 replica_id (LE)] [u64 previous_min] [u64 proposed]
            OpResult::ActiveSnapshotRejected { replica_id, previous_min, proposed } => {
                b.push(14);
                codec::put_u32(&mut b, *replica_id);
                codec::put_u64(&mut b, *previous_min);
                codec::put_u64(&mut b, *proposed);
            }
            // SP-PG-SERIAL-RETURNING: Created (15) wire encode.
            //   wire: [u128 assigned id (LE)]
            OpResult::Created { id } => {
                b.push(15);
                b.extend_from_slice(&id.to_le_bytes());
            }
            // SP-PG-RETURNING-MULTIROW-STAR: CreatedMany (16) wire encode.
            //   wire: [u32 count (LE)] [repeat count: u128 id (LE)]
            OpResult::CreatedMany { ids } => {
                b.push(16);
                codec::put_u32(&mut b, ids.len() as u32);
                for id in ids {
                    b.extend_from_slice(&id.to_le_bytes());
                }
            }
        }
        b
    }

    pub fn decode(buf: &[u8]) -> Option<OpResult> {
        let mut c = codec::Cursor::new(buf);
        Some(match c.u8()? {
            0 => OpResult::Ok,
            // SP-Perf-A T6 Fix B: decode wraps the freshly-read Vec into an
            // Arc<[u8]> once at the wire boundary; downstream clones bump
            // the refcount instead of allocating.
            1 => OpResult::Got(Arc::from(c.bytes()?)),
            2 => OpResult::Exists,
            3 => OpResult::NotFound,
            4 => OpResult::TypeCreated(c.u32()?),
            5 => OpResult::SchemaError(String::from_utf8_lossy(&c.bytes()?).into_owned()),
            6 => OpResult::Constraint(String::from_utf8_lossy(&c.bytes()?).into_owned()),
            7 => OpResult::Unavailable,
            8 => OpResult::Unauthorized,
            // SP112 T2: TxCommitted (9) + TxAborted (10) wire decode.
            9 => OpResult::TxCommitted { commit_opnum: c.u64()? },
            10 => {
                let reason = match c.u8()? {
                    0 => AbortReason::SnapshotOutOfRange,
                    1 => {
                        let type_id = c.u32()?;
                        let object_id = c.object_id()?.0;
                        AbortReason::WriteWriteConflict { type_id, object_id }
                    }
                    2 => AbortReason::StorageIo { kind: c.u32()? as i32 },
                    // SP113 / S2.4: DangerousStructure at sub-tag 3.
                    3 => AbortReason::DangerousStructure { other_commit_opnum: c.u64()? },
                    _ => return None,
                };
                OpResult::TxAborted { reason }
            }
            // SP114 / S2.5: WatermarkAdvanced (11) + WatermarkRejected (12).
            11 => {
                let new_low_water_mark = c.u64()?;
                let versions_deleted = c.u64()? as usize;
                let pending_txs_evicted = c.u64()? as usize;
                OpResult::WatermarkAdvanced { new_low_water_mark, versions_deleted, pending_txs_evicted }
            }
            12 => {
                let reason = match c.u8()? {
                    0 => WatermarkRejection::NotMonotonic {
                        proposed: c.u64()?,
                        current: c.u64()?,
                    },
                    1 => WatermarkRejection::AboveCommitCeiling {
                        proposed: c.u64()?,
                        current_commit: c.u64()?,
                    },
                    _ => return None,
                };
                OpResult::WatermarkRejected { reason }
            }
            // SP123 / S2.X
            13 => OpResult::ActiveSnapshotReported {
                replica_id: c.u32()?,
                accepted_min: c.u64()?,
            },
            14 => OpResult::ActiveSnapshotRejected {
                replica_id: c.u32()?,
                previous_min: c.u64()?,
                proposed: c.u64()?,
            },
            // SP-PG-SERIAL-RETURNING: Created (15) wire decode.
            15 => OpResult::Created { id: c.u128()? },
            // SP-PG-RETURNING-MULTIROW-STAR: CreatedMany (16) wire decode.
            16 => {
                let count = c.u32()? as usize;
                let mut ids = Vec::with_capacity(count);
                for _ in 0..count {
                    ids.push(c.u128()?);
                }
                OpResult::CreatedMany { ids }
            }
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
            Op::CommitTx { .. } => 44,
            // SP114 / S2.5: GC watermark advance op at wire tag 45.
            Op::AdvanceWatermark { .. } => 45,
            // SP123 / S2.X: per-replica active-snapshot report at wire tag 46.
            Op::ReportActiveSnapshot { .. } => 46,
            // SP-Analytic-Plan-MULTI: multi-aggregate single-scan GROUP BY
            // at wire tag 47 (next free).
            Op::GroupAggregateMulti { .. } => 47,
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
                | Op::GroupAggregateMulti { .. }
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
                rows_path, pagination, objstore,
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
                // Additive (pagination follow-on): a None/None op appends
                // two trailing zero tag bytes; an OLD slice-1 frame has
                // neither and the tolerant decode treats its absence as
                // None/None (see decode arm).
                match rows_path {
                    None => b.push(0),
                    Some(s) => { b.push(1); codec::put_bytes(&mut b, s.as_bytes()); }
                }
                // Tags MUST match kessel_catalog::PaginationRecipe wire encoding.
                match pagination {
                    None => b.push(0),
                    Some((tag, a, c)) => {
                        b.push(*tag);
                        codec::put_bytes(&mut b, a.as_bytes());
                        codec::put_bytes(&mut b, c.as_bytes());
                    }
                }
                // Additive (OBJ-1): None ⇒ one trailing 0 tag byte; an
                // OLD frame has neither and the tolerant decode treats
                // its absence as None.
                match objstore {
                    None => b.push(0),
                    Some((prov, acct, region, endpoint)) => {
                        b.push(1);
                        b.push(*prov);
                        codec::put_bytes(&mut b, acct.as_bytes());
                        codec::put_bytes(&mut b, region.as_bytes());
                        codec::put_bytes(&mut b, endpoint.as_bytes());
                    }
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
            Op::Join {
                left_type,
                right_type,
                left_field,
                right_field,
                limit,
                filter,
                join_type,
                order_by,
                limit_n,
                offset_n,
                group_aggregate,
                extra_joins,
            } => {
                codec::put_u32(&mut b, *left_type);
                codec::put_u32(&mut b, *right_type);
                b.extend_from_slice(&left_field.to_le_bytes());
                b.extend_from_slice(&right_field.to_le_bytes());
                codec::put_u32(&mut b, *limit);
                // SP-PG-SQL-JOIN-WHERE / -OUTER-JOIN / -JOIN-QUERY: up to THREE
                // OPTIONAL trailing regions. The combined-schema filter
                // (length-prefixed) comes first, then a single join-type tag
                // byte, then the sort/page block. Each is emitted only when it
                // carries non-default information, so a bare INNER join (no
                // filter, no pagination) is byte-identical to the pre-arc
                // frame. A region that is absent on its own but PRECEDES a
                // present later region is force-written as its empty/default
                // anchor so the positional decode stays unambiguous.
                let non_inner = *join_type != JoinType::Inner;
                let has_page =
                    order_by.is_some() || limit_n.is_some() || offset_n.is_some();
                // SP-PG-SQL-JOIN-AGG: a FOURTH optional trailing region after the
                // page block. When present it FORCES every earlier region as a
                // positional anchor so the decode stays unambiguous.
                let has_ga = group_aggregate.is_some();
                // SP-PG-SQL-MULTI-JOIN: a FIFTH optional region (chained extra
                // joins) positioned AFTER the page block and BEFORE the ga block.
                // When present it FORCES the filter / join_type / page anchors so
                // the positional decode stays unambiguous; empty ⇒ writes nothing
                // (a 2-table join is byte-identical to a pre-arc frame).
                let has_mj = !extra_joins.is_empty();
                if !filter.is_empty() || non_inner || has_page || has_ga || has_mj {
                    codec::put_bytes(&mut b, filter);
                }
                if non_inner || has_page || has_ga || has_mj {
                    b.push(join_type.wire_tag());
                }
                // SP-PG-SQL-JOIN-QUERY: page block, guarded by a marker byte so
                // an old/inner frame (no trailing bytes) decodes to all-None.
                // Force-written (all-None) when a group-aggregate or multi-join
                // block follows.
                if has_page || has_ga || has_mj {
                    b.push(1u8); // page-block marker
                    match order_by {
                        Some((f, desc)) => {
                            b.push(1u8);
                            b.extend_from_slice(&f.to_le_bytes());
                            b.push(*desc as u8);
                        }
                        None => b.push(0u8),
                    }
                    match limit_n {
                        Some(n) => {
                            b.push(1u8);
                            b.extend_from_slice(&n.to_le_bytes());
                        }
                        None => b.push(0u8),
                    }
                    match offset_n {
                        Some(n) => {
                            b.push(1u8);
                            b.extend_from_slice(&n.to_le_bytes());
                        }
                        None => b.push(0u8),
                    }
                }
                // SP-PG-SQL-MULTI-JOIN: chained extra-join block, AFTER the page
                // block and sharing the post-page position with the ga block.
                // `encode_extra_joins` writes `[2][count][steps…]` when non-empty
                // and NOTHING when empty. Its marker byte (`2`) differs from the
                // ga marker (`1`), so the decoder distinguishes the two WITHOUT a
                // presence anchor ⇒ a ga-only frame stays byte-identical to a
                // pre-arc frame. V1 never emits both blocks together.
                encode_extra_joins(&mut b, extra_joins);
                // SP-PG-SQL-JOIN-AGG: group-aggregate block, guarded by its own
                // marker. Emitted only when `group_aggregate` is Some, so a join
                // without it writes NOTHING here ⇒ byte-identical to the pre-arc
                // frame. A non-1 marker is a forward-incompatible op (rejected at
                // decode). n_aggs is non-empty by construction (the SQL layer
                // never emits an empty aggregate list).
                if let Some(ga) = group_aggregate {
                    b.push(1u8); // ga-block marker
                    b.extend_from_slice(&ga.group_field.to_le_bytes());
                    b.extend_from_slice(&(ga.aggregates.len() as u16).to_le_bytes());
                    for (kind, fid) in &ga.aggregates {
                        b.push(*kind);
                        b.extend_from_slice(&fid.to_le_bytes());
                    }
                    // SP-PG-SQL-HAVING: marker-guarded HAVING block, INSIDE the
                    // ga-block (only reachable when group_aggregate is Some, so a
                    // plain/sorted/filtered join writes nothing here). Absent ⇒
                    // a pre-HAVING join-group-aggregate frame is byte-identical.
                    encode_having(&mut b, &ga.having);
                }
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
            Op::Aggregate { type_id, program, kind, field_id, range_preds } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_bytes(&mut b, program);
                b.push(*kind);
                b.extend_from_slice(&field_id.to_le_bytes());
                // SP-Analytic-Plan: trailing range hints. Empty ⇒ omit
                // entirely so a pre-arc frame is byte-identical (back-
                // compat). Decode tolerates the absence.
                if !range_preds.is_empty() {
                    codec::put_u32(&mut b, range_preds.len() as u32);
                    for (f, o, v) in range_preds {
                        b.extend_from_slice(&f.to_le_bytes());
                        b.push(*o);
                        codec::put_bytes(&mut b, v);
                    }
                }
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
            Op::GroupAggregate { type_id, program, group_field, kind, agg_field, range_preds, having, sort } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_bytes(&mut b, program);
                b.extend_from_slice(&group_field.to_le_bytes());
                b.push(*kind);
                b.extend_from_slice(&agg_field.to_le_bytes());
                // SP-PG-SQL-HAVING / -GROUP-SORT-LIMIT: a HAVING clause OR a
                // group-sort block forces the range-preds length prefix to be
                // written even when empty (a `0u32`) so the trailing
                // HAVING/sort blocks have a fixed offset to follow. A query
                // with NO range hints AND NO HAVING AND NO sort still omits
                // both ⇒ byte-identical to the pre-arc frame.
                if !range_preds.is_empty() || having.is_some() || sort.is_some() {
                    codec::put_u32(&mut b, range_preds.len() as u32);
                    for (f, o, v) in range_preds {
                        b.extend_from_slice(&f.to_le_bytes());
                        b.push(*o);
                        codec::put_bytes(&mut b, v);
                    }
                }
                // SP-PG-SQL-HAVING + -GROUP-SORT-LIMIT: marker-guarded trailing
                // HAVING block then group-sort block (see encode_group_trailer).
                // Both absent ⇒ NOTHING written here ⇒ byte-identical to before.
                encode_group_trailer(&mut b, having, sort);
            }
            // SP-Analytic-Plan-MULTI: wire tag 47.
            //   [u32 type_id]
            //   [u32 prog_len][prog]
            //   [u16 group_field]
            //   [u32 n_aggs] { [u8 kind][u16 field_id] }*
            //   [u32 n_range_preds] { [u16 f][u8 op][u32 v_len][v] }*
            // n_range_preds is REQUIRED (no back-compat omission since
            // this variant is brand-new; reader symmetry is simpler).
            Op::GroupAggregateMulti { type_id, program, group_field, aggregates, range_preds, having, sort } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_bytes(&mut b, program);
                b.extend_from_slice(&group_field.to_le_bytes());
                codec::put_u32(&mut b, aggregates.len() as u32);
                for (k, f) in aggregates {
                    b.push(*k);
                    b.extend_from_slice(&f.to_le_bytes());
                }
                codec::put_u32(&mut b, range_preds.len() as u32);
                for (f, o, v) in range_preds {
                    b.extend_from_slice(&f.to_le_bytes());
                    b.push(*o);
                    codec::put_bytes(&mut b, v);
                }
                // SP-PG-SQL-HAVING + -GROUP-SORT-LIMIT: marker-guarded trailing
                // HAVING block then group-sort block. Tag 47's range_preds
                // always carried an explicit length, so these simply follow;
                // both absent ⇒ NOTHING written (byte-identical to a
                // pre-HAVING/pre-sort tag-47 frame).
                encode_group_trailer(&mut b, having, sort);
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
            Op::CommitTx { snapshot_opnum, write_set, commit_opnum, read_set } => {
                // wire: [u64 snapshot_opnum][u32 write_set_len]
                //       { [u32 type_id][16B object_id][u8 presence][?bytes value] }*
                //       [u64 commit_opnum]
                //       [u32 read_set_len] { [u32 type_id][16B object_id] }*
                // presence byte: 0 = tombstone (None), 1 = live (Some(value follows))
                // SP113 / S2.4: read_set appended after commit_opnum (additive;
                // absent bytes in SP112-shaped frames are decoded as empty vec).
                b.extend_from_slice(&snapshot_opnum.to_le_bytes());
                codec::put_u32(&mut b, write_set.len() as u32);
                for (type_id, object_id, value) in write_set {
                    codec::put_u32(&mut b, *type_id);
                    b.extend_from_slice(object_id);
                    match value {
                        None => b.push(0),
                        Some(v) => {
                            b.push(1);
                            codec::put_bytes(&mut b, v);
                        }
                    }
                }
                b.extend_from_slice(&commit_opnum.to_le_bytes());
                // SP113 / S2.4: read_set suffix. Empty vec = SP112 compat default.
                codec::put_u32(&mut b, read_set.len() as u32);
                for (type_id, object_id) in read_set {
                    codec::put_u32(&mut b, *type_id);
                    b.extend_from_slice(object_id);
                }
            }
            // SP114 / S2.5: wire tag 45 — [u64 low_water_mark] (LE).
            // Uses the same put_u64 helper as CommitTx's commit_opnum field.
            Op::AdvanceWatermark { low_water_mark } => {
                codec::put_u64(&mut b, *low_water_mark);
            }
            // SP123 / S2.X: wire tag 46 — [u32 replica_id (LE)] +
            // [u64 min_active_snapshot (LE)].
            Op::ReportActiveSnapshot { replica_id, min_active_snapshot } => {
                codec::put_u32(&mut b, *replica_id);
                codec::put_u64(&mut b, *min_active_snapshot);
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
                // Tolerant back-compat decode (WAL-replay critical): an OLD
                // slice-1 frame ends right after `mapping`, so the cursor is
                // exhausted here and `c.u8()` returns `None` -> slice-1
                // default of None/None (NOT a decode failure). Cursor readers
                // return Option<T>, so `None` distinguishes "no trailing
                // bytes" (slice-1) from a present tag byte.
                let rows_path = match c.u8() {
                    None | Some(0) => None,
                    Some(1) => Some(String::from_utf8_lossy(&c.bytes()?).into_owned()),
                    // Unknown PRESENT tag ⇒ cursor position is unknowable;
                    // fail the decode (matches kessel_catalog's stance).
                    // An EXHAUSTED cursor (None) stays the slice-1 default.
                    Some(_) => return None,
                };
                let pagination = match c.u8() {
                    None | Some(0) => None,
                    Some(t @ 1..=3) => {
                        let a = String::from_utf8_lossy(&c.bytes()?).into_owned();
                        let cc = String::from_utf8_lossy(&c.bytes()?).into_owned();
                        Some((t, a, cc))
                    }
                    Some(_) => return None,
                };
                let objstore = match c.u8() {
                    None | Some(0) => None,
                    Some(1) => {
                        let prov = c.u8()?;
                        let acct = String::from_utf8_lossy(&c.bytes()?).into_owned();
                        let region = String::from_utf8_lossy(&c.bytes()?).into_owned();
                        let endpoint = String::from_utf8_lossy(&c.bytes()?).into_owned();
                        Some((prov, acct, region, endpoint))
                    }
                    // Unknown PRESENT tag ⇒ fail (matches the rows_path/
                    // pagination stance; an EXHAUSTED cursor (None) stays
                    // the slice-1/None default).
                    Some(_) => return None,
                };
                Op::CreateExternalSource {
                    name, type_def, url, format, key_field_id,
                    auth_kind, auth_a, auth_b, mapping,
                    rows_path, pagination, objstore,
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
            28 => {
                let left_type = c.u32()?;
                let right_type = c.u32()?;
                let left_field = c.u16()?;
                let right_field = c.u16()?;
                let limit = c.u32()?;
                // SP-PG-SQL-JOIN-WHERE: optional trailing combined-schema
                // filter (length-prefixed). Absent (older / bare-join frame)
                // ⇒ empty ⇒ identical behaviour to before.
                let filter = if c.remaining() > 0 { c.bytes()? } else { Vec::new() };
                // SP-PG-SQL-OUTER-JOIN: optional trailing join-type tag byte.
                // Absent ⇒ Inner (every pre-arc / inner frame). Present and
                // unknown ⇒ decode failure (forward-incompatible op rejected
                // rather than silently mis-applied).
                let join_type = if c.remaining() > 0 {
                    JoinType::from_wire_tag(c.u8()?)?
                } else {
                    JoinType::Inner
                };
                // SP-PG-SQL-JOIN-QUERY: optional trailing sort/page block,
                // guarded by a marker byte. Absent (older / non-paginated
                // frame) ⇒ all-None. A non-1 marker is a forward-incompatible
                // op ⇒ decode failure (surfaced, not silently mis-applied).
                let (order_by, limit_n, offset_n) = if c.remaining() > 0 {
                    if c.u8()? != 1 {
                        return None;
                    }
                    let order_by = if c.u8()? != 0 {
                        Some((c.u16()?, c.u8()? != 0))
                    } else {
                        None
                    };
                    let limit_n = if c.u8()? != 0 { Some(c.u64()?) } else { None };
                    let offset_n = if c.u8()? != 0 { Some(c.u64()?) } else { None };
                    (order_by, limit_n, offset_n)
                } else {
                    (None, None, None)
                };
                // SP-PG-SQL-MULTI-JOIN + SP-PG-SQL-JOIN-AGG: the post-page-block
                // region holds AT MOST one of two mutually-exclusive (V1) blocks,
                // distinguished by their FIRST marker byte: `2` ⇒ chained
                // extra-joins block; `1` ⇒ group-aggregate block. Absent (older /
                // 2-table non-grouped frame) ⇒ neither (empty extra_joins / None
                // ga). Any other marker is a forward-incompatible op ⇒ fail.
                let mut extra_joins: Vec<JoinStep> = Vec::new();
                let mut group_aggregate: Option<JoinGroupAgg> = None;
                if c.remaining() > 0 {
                    match c.peek_u8()? {
                        EXTRA_JOINS_MARKER => {
                            c.u8()?; // consume the extra-joins marker
                            extra_joins = read_extra_joins_body(&mut c).ok()?;
                        }
                        1 => {
                            c.u8()?; // consume the ga-block marker
                            let group_field = c.u16()?;
                            let n = c.u16()? as usize;
                            if n == 0 {
                                return None;
                            }
                            let mut aggregates = Vec::with_capacity(n);
                            for _ in 0..n {
                                let k = c.u8()?;
                                let f = c.u16()?;
                                aggregates.push((k, f));
                            }
                            // SP-PG-SQL-HAVING: optional HAVING block INSIDE the
                            // ga-block. Absent ⇒ None.
                            let having = decode_having(&mut c).ok()?;
                            group_aggregate = Some(JoinGroupAgg { group_field, aggregates, having });
                        }
                        _ => return None,
                    }
                }
                Op::Join {
                    left_type,
                    right_type,
                    left_field,
                    right_field,
                    limit,
                    filter,
                    join_type,
                    order_by,
                    limit_n,
                    offset_n,
                    group_aggregate,
                    extra_joins,
                }
            }
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
            20 => {
                let type_id = c.u32()?;
                let program = c.bytes()?;
                let kind = c.u8()?;
                let field_id = c.u16()?;
                // SP-Analytic-Plan: optional trailing range hints.
                // Absent (older frame) ⇒ empty ⇒ identical behaviour.
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
                Op::Aggregate { type_id, program, kind, field_id, range_preds }
            }
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
            22 => {
                let type_id = c.u32()?;
                let program = c.bytes()?;
                let group_field = c.u16()?;
                let kind = c.u8()?;
                let agg_field = c.u16()?;
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
                // SP-PG-SQL-HAVING: optional trailing HAVING block. Absent ⇒
                // None (pre-arc frame). The range-preds prefix above is written
                // (possibly as a `0`) whenever HAVING/sort is present, so any
                // remaining bytes here ARE the HAVING then group-sort blocks.
                let having = decode_having(&mut c).ok()?;
                // SP-PG-SQL-GROUP-SORT-LIMIT: optional trailing group-sort block.
                let sort = decode_group_sort(&mut c).ok()?;
                Op::GroupAggregate { type_id, program, group_field, kind, agg_field, range_preds, having, sort }
            }
            // SP-Analytic-Plan-MULTI: wire tag 47.
            47 => {
                let type_id = c.u32()?;
                let program = c.bytes()?;
                let group_field = c.u16()?;
                let n = c.u32()? as usize;
                if n == 0 {
                    // N=0 aggregates makes no semantic sense (the result
                    // encoding has nothing per group). Reject at decode.
                    return None;
                }
                let mut aggregates = Vec::with_capacity(n);
                for _ in 0..n {
                    let k = c.u8()?;
                    let f = c.u16()?;
                    aggregates.push((k, f));
                }
                let m = c.u32()? as usize;
                let mut range_preds = Vec::with_capacity(m);
                for _ in 0..m {
                    range_preds.push((c.u16()?, c.u8()?, c.bytes()?));
                }
                // SP-PG-SQL-HAVING: optional trailing HAVING block. Tag 47
                // always wrote the range-preds length, so any remaining bytes
                // are the HAVING then group-sort blocks. Absent ⇒ None.
                let having = decode_having(&mut c).ok()?;
                // SP-PG-SQL-GROUP-SORT-LIMIT: optional trailing group-sort block.
                let sort = decode_group_sort(&mut c).ok()?;
                Op::GroupAggregateMulti { type_id, program, group_field, aggregates, range_preds, having, sort }
            }
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
            44 => {
                let snapshot_opnum = c.u64()?;
                let n = c.u32()? as usize;
                let mut write_set = Vec::with_capacity(n);
                for _ in 0..n {
                    let type_id = c.u32()?;
                    // Decode the 16-byte object_id via the Cursor::object_id helper;
                    // the helper returns ObjectId(bytes) so we take its inner array.
                    let oid = c.object_id()?;
                    let object_id: [u8; 16] = oid.0;
                    let presence = c.u8()?;
                    let value = match presence {
                        0 => None,
                        1 => Some(c.bytes()?),
                        _ => return None,
                    };
                    write_set.push((type_id, object_id, value));
                }
                let commit_opnum = c.u64()?;
                // SP113 / S2.4: read_set suffix — additive. If buffer is
                // exhausted here, this is an SP112-shaped frame → treat
                // read_set as vec![] for backward read-compat.
                let read_set = if c.remaining() == 0 {
                    vec![]
                } else {
                    let n = c.u32()? as usize;
                    let mut rs = Vec::with_capacity(n);
                    for _ in 0..n {
                        let type_id = c.u32()?;
                        let object_id = c.object_id()?.0;
                        rs.push((type_id, object_id));
                    }
                    rs
                };
                Op::CommitTx { snapshot_opnum, write_set, commit_opnum, read_set }
            }
            // SP114 / S2.5: wire tag 45 — [u64 low_water_mark] (LE).
            45 => Op::AdvanceWatermark { low_water_mark: c.u64()? },
            // SP123 / S2.X: wire tag 46 — [u32 replica_id] + [u64 min].
            46 => Op::ReportActiveSnapshot {
                replica_id: c.u32()?,
                min_active_snapshot: c.u64()?,
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
        /// Read the next byte WITHOUT advancing (SP-PG-SQL-MULTI-JOIN: lets the
        /// Op::Join decode distinguish the extra-joins block (marker 2) from the
        /// ga block (marker 1) that share the same post-page-block position).
        pub fn peek_u8(&self) -> Option<u8> {
            self.buf.get(self.pos).copied()
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
        pub fn u128(&mut self) -> Option<u128> {
            let s = self.buf.get(self.pos..self.pos + 16)?;
            self.pos += 16;
            Some(u128::from_le_bytes(s.try_into().ok()?))
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
            Op::Join { left_type: 4, right_type: 5, left_field: 1, right_field: 2, limit: 9, filter: vec![], join_type: JoinType::Inner, order_by: None, limit_n: None, offset_n: None, group_aggregate: None, extra_joins: vec![] },
            // SP-PG-SQL-JOIN-WHERE: filtered join — non-empty filter program
            // round-trips through the new optional trailing wire suffix.
            Op::Join { left_type: 4, right_type: 5, left_field: 1, right_field: 2, limit: 9, filter: vec![1, 0, 0, 5, 42, 3], join_type: JoinType::Inner, order_by: None, limit_n: None, offset_n: None, group_aggregate: None, extra_joins: vec![] },
            // SP-PG-SQL-OUTER-JOIN: LEFT join, no filter — the join-type tag
            // round-trips with an empty filter ahead of it.
            Op::Join { left_type: 4, right_type: 5, left_field: 1, right_field: 2, limit: 9, filter: vec![], join_type: JoinType::Left, order_by: None, limit_n: None, offset_n: None, group_aggregate: None, extra_joins: vec![] },
            // SP-PG-SQL-OUTER-JOIN: LEFT join WITH filter — both trailing
            // fields present (filter then tag).
            Op::Join { left_type: 4, right_type: 5, left_field: 1, right_field: 2, limit: 9, filter: vec![1, 0, 0, 5, 42, 3], join_type: JoinType::Left, order_by: None, limit_n: None, offset_n: None, group_aggregate: None, extra_joins: vec![] },
            // SP-PG-SQL-JOIN-QUERY: ORDER BY only (asc) — page block with just
            // the sort field, limit/offset absent.
            Op::Join { left_type: 4, right_type: 5, left_field: 1, right_field: 2, limit: 0, filter: vec![], join_type: JoinType::Inner, order_by: Some((3, false)), limit_n: None, offset_n: None, group_aggregate: None, extra_joins: vec![] },
            // SP-PG-SQL-JOIN-QUERY: ORDER BY DESC + LIMIT + OFFSET — all page
            // fields present, over an INNER join with no filter.
            Op::Join { left_type: 4, right_type: 5, left_field: 1, right_field: 2, limit: 0, filter: vec![], join_type: JoinType::Inner, order_by: Some((6, true)), limit_n: Some(20), offset_n: Some(40), group_aggregate: None, extra_joins: vec![] },
            // SP-PG-SQL-JOIN-QUERY: LIMIT/OFFSET with NO order_by, over a LEFT
            // join WITH filter — every trailing region present at once.
            Op::Join { left_type: 4, right_type: 5, left_field: 1, right_field: 2, limit: 0, filter: vec![1, 0, 0, 5, 42, 3], join_type: JoinType::Left, order_by: None, limit_n: Some(5), offset_n: Some(2), group_aggregate: None, extra_joins: vec![] },
            // SP-PG-SQL-JOIN-AGG: GROUP BY combined field 0, single COUNT(*)
            // aggregate (sentinel field id) over an INNER join, no filter — the
            // ga block force-writes the empty-filter + inner-tag + all-None page
            // block anchors, then the group/agg fields.
            Op::Join { left_type: 4, right_type: 5, left_field: 1, right_field: 2, limit: 0, filter: vec![], join_type: JoinType::Inner, order_by: None, limit_n: None, offset_n: None, group_aggregate: Some(JoinGroupAgg { group_field: 0, aggregates: vec![(0, COUNT_STAR_FIELD)], having: None }), extra_joins: vec![] },
            // SP-PG-SQL-JOIN-AGG: GROUP BY field 1, TWO aggregates (COUNT(col 3)
            // + SUM(col 4)) over a LEFT join WITH filter — every trailing region
            // present at once (filter, tag, page block, ga block).
            Op::Join { left_type: 4, right_type: 5, left_field: 1, right_field: 2, limit: 0, filter: vec![1, 0, 0, 5, 42, 3], join_type: JoinType::Left, order_by: None, limit_n: None, offset_n: None, group_aggregate: Some(JoinGroupAgg { group_field: 1, aggregates: vec![(0, 3), (1, 4)], having: None }), extra_joins: vec![] },
            Op::Aggregate { type_id: 4, program: vec![1], kind: 1, field_id: 3, range_preds: vec![] },
            // SP-Analytic-Plan: aggregate w/ range hints — new wire suffix.
            Op::Aggregate { type_id: 4, program: vec![1], kind: 1, field_id: 3, range_preds: vec![(2, 1, vec![7, 0]), (2, 3, vec![9, 0])] },
            Op::SelectFields { type_id: 4, program: vec![1], fields: vec![1, 3], limit: 5 },
            Op::GroupAggregate { type_id: 4, program: vec![1], group_field: 1, kind: 1, agg_field: 3, range_preds: vec![], having: None, sort: None },
            // SP-Analytic-Plan: group-agg w/ range hints — new wire suffix.
            Op::GroupAggregate { type_id: 4, program: vec![1], group_field: 1, kind: 1, agg_field: 3, range_preds: vec![(2, 1, vec![1, 0]), (2, 3, vec![5, 0])], having: None, sort: None },
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
            // SP112 T1: CommitTx scaffold wire roundtrip (read_set: vec![] = SP112 compat).
            Op::CommitTx {
                snapshot_opnum: 7,
                write_set: vec![
                    (1u32, [0u8; 16], Some(vec![0xAA, 0xBB])),
                    (2u32, {let mut k=[0u8;16]; k[15]=5; k}, None),
                ],
                commit_opnum: 42,
                read_set: vec![],
            },
            // Empty write_set edge case (read_set: vec![] = SP112 compat).
            Op::CommitTx { snapshot_opnum: 0, write_set: vec![], commit_opnum: 0, read_set: vec![] },
            // SP113 T1: CommitTx with non-empty read_set (SSI frame).
            Op::CommitTx {
                snapshot_opnum: 7,
                write_set: vec![],
                commit_opnum: 11,
                read_set: vec![(2u32, [3u8; 16]), (4u32, [5u8; 16])],
            },
        ];
        for op in ops {
            let enc = op.encode();
            let dec = Op::decode(&enc).expect("decode");
            assert_eq!(op, dec);
            assert_eq!(op.kind(), enc[0]);
        }
    }

    /// SP-PG-SQL-OUTER-JOIN regression: an INNER bare join (no filter) must
    /// encode byte-IDENTICALLY to the pre-arc frame — i.e. NO trailing
    /// join-type tag byte is appended. This guarantees every existing inner
    /// join is wire-unchanged and old logs replay identically.
    #[test]
    fn inner_join_no_filter_wire_byte_identical() {
        let inner = Op::Join {
            left_type: 4, right_type: 5, left_field: 1, right_field: 2,
            limit: 9, filter: vec![], join_type: JoinType::Inner,
            order_by: None, limit_n: None, offset_n: None, group_aggregate: None,
            extra_joins: vec![],
        };
        let enc = inner.encode();
        // tag(28) + lt(u32) + rt(u32) + lf(u16) + rf(u16) + limit(u32)
        // = 1 + 4 + 4 + 2 + 2 + 4 = 17 bytes, NO trailing suffix.
        assert_eq!(enc.len(), 17, "inner bare join must have no trailing suffix");
        assert_eq!(Op::decode(&enc).expect("decode"), inner);
    }

    /// SP-PG-SQL-OUTER-JOIN: a LEFT join with no filter appends an empty
    /// filter (len-0, 4 bytes) followed by the join-type tag (1 byte), and
    /// round-trips back to `JoinType::Left`.
    #[test]
    fn left_join_no_filter_carries_tag() {
        let left = Op::Join {
            left_type: 4, right_type: 5, left_field: 1, right_field: 2,
            limit: 9, filter: vec![], join_type: JoinType::Left,
            order_by: None, limit_n: None, offset_n: None, group_aggregate: None,
            extra_joins: vec![],
        };
        let enc = left.encode();
        // 17 base + 4 (empty filter len) + 1 (tag) = 22.
        assert_eq!(enc.len(), 22, "left join carries empty filter + tag");
        assert_eq!(*enc.last().unwrap(), JoinType::Left.wire_tag());
        assert_eq!(Op::decode(&enc).expect("decode"), left);
    }

    /// An unknown join-type tag is rejected at decode (forward-incompat op is
    /// surfaced, not silently mis-applied as inner).
    #[test]
    fn unknown_join_type_tag_rejected() {
        let left = Op::Join {
            left_type: 4, right_type: 5, left_field: 1, right_field: 2,
            limit: 9, filter: vec![], join_type: JoinType::Left,
            order_by: None, limit_n: None, offset_n: None, group_aggregate: None,
            extra_joins: vec![],
        };
        let mut enc = left.encode();
        *enc.last_mut().unwrap() = 0x7F; // bogus tag
        assert!(Op::decode(&enc).is_none(), "unknown join-type tag must fail decode");
    }

    /// SP-PG-SQL-JOIN-QUERY regression: an INNER bare join with NO pagination
    /// must STILL encode byte-identically to the pre-arc 17-byte frame — the
    /// page block is omitted entirely when order_by/limit_n/offset_n are all
    /// None, so no marker byte leaks in.
    #[test]
    fn join_no_pagination_wire_byte_identical() {
        let inner = Op::Join {
            left_type: 4, right_type: 5, left_field: 1, right_field: 2,
            limit: 9, filter: vec![], join_type: JoinType::Inner,
            order_by: None, limit_n: None, offset_n: None, group_aggregate: None,
            extra_joins: vec![],
        };
        assert_eq!(inner.encode().len(), 17, "no page block ⇒ 17-byte frame");
    }

    /// SP-PG-SQL-JOIN-QUERY: a paginated INNER join (ORDER BY asc + LIMIT +
    /// OFFSET, no filter) force-writes the empty filter + inner tag anchors,
    /// then the page block, and round-trips exactly.
    #[test]
    fn paginated_join_round_trips() {
        let op = Op::Join {
            left_type: 4, right_type: 5, left_field: 1, right_field: 2,
            limit: 0, filter: vec![], join_type: JoinType::Inner,
            order_by: Some((3, false)), limit_n: Some(20), offset_n: Some(40), group_aggregate: None,
            extra_joins: vec![],
        };
        let enc = op.encode();
        // 17 base + 4 (empty filter len) + 1 (inner tag=0) + page block:
        // marker(1) + has_order(1)+field(2)+desc(1) + has_limit(1)+u64(8)
        // + has_offset(1)+u64(8) = 17 + 4 + 1 + 1 + 4 + 9 + 9 = 45.
        assert_eq!(enc.len(), 45, "paginated join frame size");
        assert_eq!(Op::decode(&enc).expect("decode"), op);
    }

    /// SP-PG-SQL-JOIN-QUERY: a corrupt page-block marker (non-1) is rejected at
    /// decode — a forward-incompatible op is surfaced, not silently dropped.
    #[test]
    fn bad_page_block_marker_rejected() {
        let op = Op::Join {
            left_type: 4, right_type: 5, left_field: 1, right_field: 2,
            limit: 0, filter: vec![], join_type: JoinType::Inner,
            order_by: Some((3, false)), limit_n: None, offset_n: None, group_aggregate: None,
            extra_joins: vec![],
        };
        let enc = op.encode();
        // The marker byte sits right after the inner tag: 17 base + 4 (filter
        // len) + 1 (tag) = index 22.
        let mut bad = enc.clone();
        bad[22] = 0x09; // corrupt marker
        assert!(Op::decode(&bad).is_none(), "bad page-block marker must fail decode");
    }

    /// SP-PG-SQL-JOIN-AGG regression: a join with NO group_aggregate (and no
    /// pagination) STILL encodes to the exact pre-arc 17-byte frame — the ga
    /// block is omitted entirely, no marker leaks in.
    #[test]
    fn join_no_group_aggregate_wire_byte_identical() {
        let inner = Op::Join {
            left_type: 4, right_type: 5, left_field: 1, right_field: 2,
            limit: 9, filter: vec![], join_type: JoinType::Inner,
            order_by: None, limit_n: None, offset_n: None, group_aggregate: None,
            extra_joins: vec![],
        };
        assert_eq!(inner.encode().len(), 17, "no ga block ⇒ 17-byte frame");
    }

    /// SP-PG-SQL-JOIN-AGG: a join-group-aggregate (GROUP BY + COUNT(*), no
    /// filter / pagination) force-writes the empty-filter + inner-tag + all-None
    /// page-block anchors, then the ga block, and round-trips exactly.
    #[test]
    fn join_group_aggregate_round_trips() {
        let op = Op::Join {
            left_type: 4, right_type: 5, left_field: 1, right_field: 2,
            limit: 0, filter: vec![], join_type: JoinType::Inner,
            order_by: None, limit_n: None, offset_n: None,
            group_aggregate: Some(JoinGroupAgg {
                group_field: 0,
                aggregates: vec![(0, COUNT_STAR_FIELD), (1, 4)],
                having: None,
            }),
            extra_joins: vec![],
        };
        let enc = op.encode();
        // 17 base + 4 (empty filter) + 1 (inner tag) + page block all-None
        // (marker(1)+has_order(1)+has_limit(1)+has_offset(1)=4) + ga block
        // (marker(1)+group_field(2)+n_aggs(2)+ 2×(kind(1)+field(2))=6 ) = 11.
        // 17 + 4 + 1 + 4 + 11 = 37.
        assert_eq!(enc.len(), 37, "join-agg frame size");
        assert_eq!(Op::decode(&enc).expect("decode"), op);
    }

    /// SP-PG-SQL-MULTI-JOIN: a join with NON-EMPTY `extra_joins` round-trips
    /// exactly (chained steps preserved). The extra-joins block (marker 2)
    /// force-writes the empty-filter + inner-tag + all-None page-block anchors.
    #[test]
    fn multi_join_round_trips() {
        let op = Op::Join {
            left_type: 1, right_type: 2, left_field: 0, right_field: 1,
            limit: 0, filter: vec![], join_type: JoinType::Inner,
            order_by: None, limit_n: None, offset_n: None, group_aggregate: None,
            extra_joins: vec![
                JoinStep { right_type: 3, left_combined_field: 2, right_field: 1 },
                JoinStep { right_type: 4, left_combined_field: 5, right_field: 0 },
            ],
        };
        let enc = op.encode();
        assert_eq!(Op::decode(&enc).expect("decode"), op, "multi-join round-trip");
        // Determinism: re-encoding the decoded op is byte-stable.
        assert_eq!(Op::decode(&enc).unwrap().encode(), enc, "multi-join byte-stable");
        // The extra-joins marker is `2` (distinct from the ga marker `1`): it
        // sits after 17 base + 4 (empty filter) + 1 (inner tag) + 4 (page
        // all-None) = index 26.
        assert_eq!(enc[26], EXTRA_JOINS_MARKER, "extra-joins marker is 2");
    }

    /// SP-PG-SQL-MULTI-JOIN regression: an EMPTY `extra_joins` adds NO bytes —
    /// a 2-table join is byte-identical to the pre-arc 17-byte frame.
    #[test]
    fn empty_extra_joins_wire_byte_identical() {
        let inner = Op::Join {
            left_type: 4, right_type: 5, left_field: 1, right_field: 2,
            limit: 9, filter: vec![], join_type: JoinType::Inner,
            order_by: None, limit_n: None, offset_n: None, group_aggregate: None,
            extra_joins: vec![],
        };
        assert_eq!(inner.encode().len(), 17, "empty extra_joins ⇒ 17-byte frame");
    }

    /// SP-PG-SQL-MULTI-JOIN: a corrupt extra-joins count (0) is rejected at
    /// decode — a malformed op is surfaced, not silently dropped.
    #[test]
    fn bad_extra_joins_count_rejected() {
        let op = Op::Join {
            left_type: 1, right_type: 2, left_field: 0, right_field: 1,
            limit: 0, filter: vec![], join_type: JoinType::Inner,
            order_by: None, limit_n: None, offset_n: None, group_aggregate: None,
            extra_joins: vec![
                JoinStep { right_type: 3, left_combined_field: 2, right_field: 1 },
            ],
        };
        let mut enc = op.encode();
        // marker(2) at 26, then u16 count at 27..29 → zero it.
        enc[27] = 0;
        enc[28] = 0;
        assert!(Op::decode(&enc).is_none(), "extra-joins count==0 must fail decode");
    }

    /// SP-PG-SQL-JOIN-AGG: a corrupt ga-block marker (non-1) is rejected at
    /// decode — a forward-incompatible op is surfaced, not silently dropped.
    #[test]
    fn bad_group_aggregate_marker_rejected() {
        let op = Op::Join {
            left_type: 4, right_type: 5, left_field: 1, right_field: 2,
            limit: 0, filter: vec![], join_type: JoinType::Inner,
            order_by: None, limit_n: None, offset_n: None,
            group_aggregate: Some(JoinGroupAgg {
                group_field: 0,
                aggregates: vec![(0, COUNT_STAR_FIELD)],
                having: None,
            }),
            extra_joins: vec![],
        };
        let enc = op.encode();
        // ga marker sits after: 17 base + 4 (filter) + 1 (tag) + 4 (page block
        // all-None) = index 26.
        let mut bad = enc.clone();
        bad[26] = 0x09;
        assert!(Op::decode(&bad).is_none(), "bad ga-block marker must fail decode");
    }

    #[test]
    fn external_source_ops_wire_round_trip() {
        for op in [
            Op::CreateExternalSource {
                name: "feed".into(), type_def: vec![1,2,3], url: "http://h/p".into(),
                format: 0, key_field_id: 2, auth_kind: 1,
                auth_a: "TOKEN_ENV".into(), auth_b: String::new(),
                mapping: vec![(1,"id".into()), (2,"k".into())],
                rows_path: None, pagination: None, objstore: None,
            },
            Op::DropExternalSource { name: "feed".into() },
            Op::RefreshExternalSource { name: "feed".into() },
        ] {
            let back = Op::decode(&op.encode()).expect("decode");
            assert_eq!(back, op, "round-trip mismatch");
            assert_eq!(op.kind(), op.encode()[0], "kind/byte mismatch");
            assert!(op.is_mutating());
        }
    }

    #[test]
    fn create_external_source_pagination_wire_round_trip() {
        for op in [
            Op::CreateExternalSource{
                name:"f".into(), type_def:vec![1], url:"u".into(), format:2,
                key_field_id:1, auth_kind:0, auth_a:String::new(), auth_b:String::new(),
                mapping:vec![(1,"id".into())],
                rows_path: Some("d.items".into()),
                pagination: Some((3, "m.c".into(), "cursor".into())),
                objstore: None,
            },
            Op::CreateExternalSource{
                name:"g".into(), type_def:vec![], url:"u2".into(), format:0,
                key_field_id:2, auth_kind:1, auth_a:"E".into(), auth_b:String::new(),
                mapping:vec![], rows_path:None,
                pagination: Some((2, String::new(), String::new())),
                objstore: None,
            },
            Op::CreateExternalSource{
                name:"h".into(), type_def:vec![9], url:"u3".into(), format:1,
                key_field_id:1, auth_kind:0, auth_a:String::new(), auth_b:String::new(),
                mapping:vec![(1,"a".into())], rows_path:None, pagination:None,
                objstore: None,
            },
        ] {
            let back = Op::decode(&op.encode()).expect("decode");
            assert_eq!(back, op);
            assert_eq!(op.kind(), op.encode()[0]);
            assert!(op.is_mutating());
        }

        // FIX: an unknown PRESENT rows_path tag must fail decode (not
        // silently corrupt the pagination field).
        let mut bad = vec![41u8];
        let put = |b:&mut Vec<u8>, s:&[u8]| { b.extend_from_slice(&(s.len() as u32).to_le_bytes()); b.extend_from_slice(s); };
        put(&mut bad, b"f");                         // name
        put(&mut bad, &[1]);                         // type_def
        put(&mut bad, b"u");                         // url
        bad.push(0);                                 // format
        bad.extend_from_slice(&1u16.to_le_bytes());  // key_field_id
        bad.push(0);                                 // auth_kind
        put(&mut bad, b"");                          // auth_a
        put(&mut bad, b"");                          // auth_b
        bad.extend_from_slice(&0u32.to_le_bytes());  // mapping len = 0
        bad.push(7);                                 // UNKNOWN rows_path tag
        assert!(Op::decode(&bad).is_none(), "unknown rows_path tag must fail decode");
    }

    #[test]
    fn create_external_source_objstore_additive_backcompat() {
        let op = Op::CreateExternalSource {
            name: "s".into(),
            type_def: vec![1, 2, 3],
            url: "s3://b/k.json".into(),
            format: 0,
            key_field_id: 1,
            auth_kind: 3,
            auth_a: "AWS_ID".into(),
            auth_b: "AWS_SECRET".into(),
            mapping: vec![(1, "id".into())],
            rows_path: None,
            pagination: None,
            objstore: Some((1, "acct".into(), "us-east-1".into(), "".into())),
        };
        let enc = op.encode();
        assert_eq!(Op::decode(&enc).unwrap(), op);

        let old = Op::CreateExternalSource {
            name: "s".into(),
            type_def: vec![1, 2, 3],
            url: "http://h".into(),
            format: 0,
            key_field_id: 1,
            auth_kind: 1,
            auth_a: "TOK".into(),
            auth_b: String::new(),
            mapping: vec![(1, "id".into())],
            rows_path: None,
            pagination: None,
            objstore: None,
        };
        let mut frame = old.encode();
        assert_eq!(*frame.last().unwrap(), 0u8, "objstore tag is last");
        frame.pop();
        let dec = Op::decode(&frame).expect("old frame decodes");
        assert_eq!(dec, old);
    }

    #[test]
    fn decodes_pre_pagination_create_external_source_frame() {
        // kind 41 + slice-1 fields ONLY, no trailing rows/pagination bytes
        // (exactly what the shipped slice-1 binary persisted to the WAL).
        let mut b = vec![41u8];
        let put = |b:&mut Vec<u8>, s:&[u8]| {
            b.extend_from_slice(&(s.len() as u32).to_le_bytes());
            b.extend_from_slice(s);
        };
        put(&mut b, b"f");              // name
        put(&mut b, &[1]);             // type_def
        put(&mut b, b"u");              // url
        b.push(0);                      // format
        b.extend_from_slice(&1u16.to_le_bytes()); // key_field_id
        b.push(0);                      // auth_kind
        put(&mut b, b"");               // auth_a
        put(&mut b, b"");               // auth_b
        b.extend_from_slice(&1u32.to_le_bytes()); // mapping len = 1
        b.extend_from_slice(&1u16.to_le_bytes()); put(&mut b, b"id"); // (fid,src)
        let op = Op::decode(&b).expect("slice-1 frame must still decode");
        match op {
            Op::CreateExternalSource{ rows_path, pagination, name, mapping, .. } => {
                assert_eq!(name, "f");
                assert_eq!(mapping, vec![(1u16,"id".to_string())]);
                assert_eq!(rows_path, None);
                assert_eq!(pagination, None);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn opresult_roundtrip_all_variants() {
        for r in [
            OpResult::Ok,
            OpResult::Got(Arc::from(vec![1, 2, 3, 250])),
            OpResult::Got(Arc::from(Vec::<u8>::new())),
            OpResult::Exists,
            OpResult::NotFound,
            OpResult::TypeCreated(77),
            OpResult::SchemaError("nope".into()),
            OpResult::Constraint("UNIQUE x".into()),
            OpResult::Unavailable,
            OpResult::Unauthorized,
            // SP-PG-SERIAL-RETURNING / -MULTIROW-STAR: Created + CreatedMany.
            OpResult::Created { id: 42 },
            OpResult::Created { id: u128::MAX },
            OpResult::CreatedMany { ids: vec![] },
            OpResult::CreatedMany { ids: vec![1] },
            OpResult::CreatedMany { ids: vec![1, 2, 3, u128::MAX] },
        ] {
            assert_eq!(OpResult::decode(&r.encode()), Some(r));
        }
        assert_eq!(OpResult::decode(&[9]), None);
        assert_eq!(OpResult::decode(&[]), None);
    }

    /// SP-PG-RETURNING-MULTIROW-STAR: CreatedMany wire shape is
    /// tag(16) + u32_le(count) + count × u128_le(id). Locks the format
    /// so a cluster of mixed-version replicas decodes it consistently.
    #[test]
    fn created_many_wire_format() {
        let r = OpResult::CreatedMany { ids: vec![7, 8] };
        let encoded = r.encode();
        let mut expected = vec![16u8];
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(&7u128.to_le_bytes());
        expected.extend_from_slice(&8u128.to_le_bytes());
        assert_eq!(encoded, expected);
        assert_eq!(OpResult::decode(&encoded), Some(r));
    }

    // ========================================================================
    // SP-Perf-A T6 Fix B — Arc<[u8]> migration regression-lock.
    //
    // The wire format of OpResult::Got MUST stay byte-identical to the
    // pre-Fix-B Vec<u8> shape (length-prefix + bytes), regardless of
    // whether the payload is materialised as Vec<u8> or Arc<[u8]>
    // internally. Locks the invariant "Got encode is shape-stable".
    // ========================================================================

    /// Wire-compat regression-lock: encode(Got(Arc::from(b"hello"))) must
    /// produce the exact byte sequence the pre-Fix-B Got(Vec<u8>) shape
    /// produced. Hand-written reference bytes: tag 1, then put_bytes which
    /// is u32 LE length (5), then the 5 ASCII bytes of "hello".
    #[test]
    fn t6_fix_b_got_wire_format_unchanged() {
        let got = OpResult::Got(Arc::from(b"hello".as_slice()));
        let encoded = got.encode();
        // Reference: tag(1) + u32_le(5) + b"hello"
        let mut expected = vec![1u8];
        expected.extend_from_slice(&5u32.to_le_bytes());
        expected.extend_from_slice(b"hello");
        assert_eq!(
            encoded, expected,
            "Fix B Got wire format must match the pre-Fix-B Vec<u8> shape \
             byte-for-byte"
        );
        // Roundtrip stays intact.
        let decoded = OpResult::decode(&encoded).expect("decode");
        assert_eq!(decoded, got);
    }

    /// Empty Got payload (Vec::new) wire-equivalent to Arc<[u8]>::from(&[]).
    #[test]
    fn t6_fix_b_got_empty_wire_format_unchanged() {
        let got = OpResult::Got(Arc::from(Vec::<u8>::new()));
        let encoded = got.encode();
        // tag(1) + u32_le(0) — no payload bytes.
        let expected = vec![1u8, 0, 0, 0, 0];
        assert_eq!(encoded, expected);
        assert_eq!(OpResult::decode(&encoded), Some(got));
    }

    /// `OpResult::Got` clones bump the Arc refcount instead of allocating.
    /// Verifies the internal sharing invariant the perf path relies on:
    /// two clones of the same `Got` point to the same heap slice.
    #[test]
    fn t6_fix_b_got_clone_shares_backing_buffer() {
        let original = OpResult::Got(Arc::from(b"shared".as_slice()));
        let dup = original.clone();
        // Extract the Arcs and compare pointer identity. If the clone had
        // allocated, the pointers would differ.
        let (a, b) = match (&original, &dup) {
            (OpResult::Got(a), OpResult::Got(b)) => (a, b),
            _ => unreachable!(),
        };
        assert!(
            Arc::ptr_eq(a, b),
            "clone of OpResult::Got must be a refcount bump, not an alloc"
        );
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

    // ========================================================================
    // SP114 / S2.5 T2 — wire roundtrip KAT (1 of 11).
    //
    // Each watermark surface (Op::AdvanceWatermark + both
    // OpResult::WatermarkAdvanced / WatermarkRejected + both
    // WatermarkRejection variants) must roundtrip byte-identically
    // (encode → decode → equal).
    // ========================================================================

    /// KAT-9 (plan): Op::AdvanceWatermark + OpResult::WatermarkAdvanced
    /// + OpResult::WatermarkRejected (both rejection variants) wire
    /// roundtrip byte-identically.
    /// Claim:    Each watermark surface encodes deterministically to a
    ///           byte string that decodes back to itself (PartialEq).
    /// Workload: For three Op::AdvanceWatermark values {0, 42, u64::MAX}
    ///           assert decode(encode(op)) == Some(op). For five
    ///           OpResult values (WatermarkAdvanced with three count
    ///           shapes + WatermarkRejected with both rejection
    ///           variants) assert decode(encode(r)) == Some(r).
    /// Expected: every roundtrip succeeds.
    #[test]
    fn kat_op_advancewatermark_wire_roundtrip() {
        // Op::AdvanceWatermark
        for lwm in [0u64, 42, u64::MAX] {
            let op = Op::AdvanceWatermark { low_water_mark: lwm };
            let bytes = op.encode();
            assert_eq!(
                Op::decode(&bytes),
                Some(op.clone()),
                "Op::AdvanceWatermark {{ lwm: {lwm} }} must roundtrip",
            );
        }
        // OpResult::WatermarkAdvanced — exercise three count shapes.
        for (lwm, dv, ev) in [(0u64, 0usize, 0usize), (42, 3, 1), (u64::MAX, 100, 50)] {
            let r = OpResult::WatermarkAdvanced {
                new_low_water_mark: lwm,
                versions_deleted: dv,
                pending_txs_evicted: ev,
            };
            assert_eq!(
                OpResult::decode(&r.encode()),
                Some(r.clone()),
                "OpResult::WatermarkAdvanced{{lwm:{lwm}, vd:{dv}, ev:{ev}}} must roundtrip",
            );
        }
        // OpResult::WatermarkRejected — both variants.
        let r_nm = OpResult::WatermarkRejected {
            reason: WatermarkRejection::NotMonotonic { proposed: 3, current: 5 },
        };
        assert_eq!(
            OpResult::decode(&r_nm.encode()),
            Some(r_nm.clone()),
            "WatermarkRejected{{NotMonotonic{{3,5}}}} must roundtrip",
        );
        let r_ac = OpResult::WatermarkRejected {
            reason: WatermarkRejection::AboveCommitCeiling {
                proposed: 1000,
                current_commit: 10,
            },
        };
        assert_eq!(
            OpResult::decode(&r_ac.encode()),
            Some(r_ac),
            "WatermarkRejected{{AboveCommitCeiling{{1000,10}}}} must roundtrip",
        );
    }

    /// SP-Analytic-Plan T1: empty `range_preds` on `Op::Aggregate` /
    /// `Op::GroupAggregate` produces the pre-arc byte-identical wire
    /// (the trailing range-hints `u32 len` is OMITTED when empty), so
    /// any SP-Analytic-Plan-PRE WAL or replication frame still decodes.
    /// A SP-Analytic-Plan-PRE encoder would have written:
    ///   [kind=20][u32 type_id][u32 prog_len][prog][u8 kind][u16 field_id]
    /// We reconstruct that exact byte stream and assert decode → empty
    /// range_preds.
    #[test]
    fn sp_analytic_plan_aggregate_wire_backcompat() {
        // ---- Op::Aggregate ----
        let post = Op::Aggregate {
            type_id: 4,
            program: vec![1, 2, 3],
            kind: 1,
            field_id: 7,
            range_preds: vec![],
        };
        let enc = post.encode();
        // Hand-roll the pre-arc bytes: same as the post-arc encoder when
        // `range_preds` is empty (no trailing u32).
        let mut hand = Vec::new();
        hand.push(20u8); // kind tag
        hand.extend_from_slice(&4u32.to_le_bytes()); // type_id
        hand.extend_from_slice(&3u32.to_le_bytes()); // prog_len
        hand.extend_from_slice(&[1, 2, 3]); // prog
        hand.push(1u8); // kind
        hand.extend_from_slice(&7u16.to_le_bytes()); // field_id
        assert_eq!(enc, hand, "empty range_preds must encode to pre-arc bytes");
        let dec = Op::decode(&hand).expect("decode pre-arc Aggregate");
        assert_eq!(dec, post, "pre-arc Aggregate decodes to empty range_preds");

        // ---- Op::GroupAggregate ----
        let post_g = Op::GroupAggregate {
            type_id: 4,
            program: vec![9],
            group_field: 2,
            kind: 0,
            agg_field: 5,
            range_preds: vec![],
            having: None, sort: None,
        };
        let enc_g = post_g.encode();
        let mut hand_g = Vec::new();
        hand_g.push(22u8); // kind tag
        hand_g.extend_from_slice(&4u32.to_le_bytes()); // type_id
        hand_g.extend_from_slice(&1u32.to_le_bytes()); // prog_len
        hand_g.extend_from_slice(&[9]); // prog
        hand_g.extend_from_slice(&2u16.to_le_bytes()); // group_field
        hand_g.push(0u8); // kind
        hand_g.extend_from_slice(&5u16.to_le_bytes()); // agg_field
        assert_eq!(enc_g, hand_g, "empty range_preds must encode to pre-arc bytes");
        let dec_g = Op::decode(&hand_g).expect("decode pre-arc GroupAggregate");
        assert_eq!(dec_g, post_g, "pre-arc GroupAggregate decodes to empty range_preds");

        // ---- Non-empty range_preds: round-trips cleanly. ----
        for rp in [
            vec![(2u16, 1u8, vec![1, 0, 0, 0])],
            vec![(2, 1, vec![1, 0]), (2, 3, vec![9, 0])],
            vec![(7, 0, vec![]), (8, 3, vec![0xFF; 8])],
        ] {
            for op in [
                Op::Aggregate {
                    type_id: 4, program: vec![1], kind: 1, field_id: 3,
                    range_preds: rp.clone(),
                },
                Op::GroupAggregate {
                    type_id: 4, program: vec![1], group_field: 1, kind: 1, agg_field: 3,
                    range_preds: rp.clone(),
                    having: None, sort: None,
                },
            ] {
                let bytes = op.encode();
                let back = Op::decode(&bytes).expect("decode non-empty range_preds");
                assert_eq!(back, op, "round-trip with range_preds={:?}", rp);
            }
        }
    }

    // ========================================================================
    // SP-Analytic-Plan-MULTI T1 — wire round-trip KAT for Op::GroupAggregateMulti.
    //
    // The new variant must encode/decode byte-identically for the canonical
    // request shapes (empty range_preds + non-empty range_preds, with
    // 2+ aggregates), AND the existing Op::Aggregate / Op::GroupAggregate
    // bytes must stay byte-identical (back-compat lock).
    // ========================================================================

    #[test]
    fn sp_analytic_plan_multi_group_aggregate_multi_wire_round_trip() {
        // Canonical Q1-shape: 4 aggregates over a 2-byte group key, with
        // a single half-range hint on the shipdate column.
        let ops = vec![
            // 2 aggregates, empty range_preds
            Op::GroupAggregateMulti {
                type_id: 4,
                program: vec![1],
                group_field: 1,
                aggregates: vec![(0, 0), (1, 3)],
                range_preds: vec![],
                having: None, sort: None,
            },
            // 4 aggregates (Q1 shape: COUNT + 3 SUMs), one range_pred
            Op::GroupAggregateMulti {
                type_id: 7,
                program: vec![1, 2, 3],
                group_field: 16,
                aggregates: vec![(0, 0), (1, 4), (1, 5), (1, 6)],
                range_preds: vec![(10, 3, vec![0x05, 0x35, 0x2F, 0x01])],
                having: None, sort: None,
            },
            // 5 aggregates incl. AVG (kind=4) + 2 range_preds
            Op::GroupAggregateMulti {
                type_id: 99,
                program: vec![],
                group_field: 0,
                aggregates: vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)],
                range_preds: vec![(2, 1, vec![1, 0]), (2, 3, vec![9, 0])],
                having: None, sort: None,
            },
        ];
        for op in ops {
            let enc = op.encode();
            assert_eq!(enc[0], 47, "wire tag must be 47");
            assert_eq!(op.kind(), 47);
            assert!(!op.is_mutating(), "GroupAggregateMulti is read-only");
            let dec = Op::decode(&enc).expect("decode GroupAggregateMulti");
            assert_eq!(dec, op, "round-trip must be byte-identical");
        }
        // N=0 aggregates: must fail decode (semantically meaningless).
        let mut bad = vec![47u8];
        bad.extend_from_slice(&4u32.to_le_bytes()); // type_id
        bad.extend_from_slice(&0u32.to_le_bytes()); // prog_len
        bad.extend_from_slice(&0u16.to_le_bytes()); // group_field
        bad.extend_from_slice(&0u32.to_le_bytes()); // n_aggs = 0
        bad.extend_from_slice(&0u32.to_le_bytes()); // n_range_preds = 0
        assert!(
            Op::decode(&bad).is_none(),
            "n_aggs=0 must be rejected at decode"
        );

        // Back-compat lock: a single Op::Aggregate frame encodes to
        // exactly the same bytes as before this arc shipped (the new
        // variant must not perturb tag-20 / tag-22 encoding).
        let agg = Op::Aggregate {
            type_id: 4, program: vec![1, 2, 3], kind: 1, field_id: 7,
            range_preds: vec![],
        };
        let agg_enc = agg.encode();
        let mut hand = Vec::new();
        hand.push(20u8);
        hand.extend_from_slice(&4u32.to_le_bytes());
        hand.extend_from_slice(&3u32.to_le_bytes());
        hand.extend_from_slice(&[1, 2, 3]);
        hand.push(1u8);
        hand.extend_from_slice(&7u16.to_le_bytes());
        assert_eq!(agg_enc, hand, "Op::Aggregate wire MUST stay byte-identical");
        let g = Op::GroupAggregate {
            type_id: 4, program: vec![9], group_field: 2, kind: 0, agg_field: 5,
            range_preds: vec![],
            having: None, sort: None,
        };
        let g_enc = g.encode();
        let mut hand_g = Vec::new();
        hand_g.push(22u8);
        hand_g.extend_from_slice(&4u32.to_le_bytes());
        hand_g.extend_from_slice(&1u32.to_le_bytes());
        hand_g.extend_from_slice(&[9]);
        hand_g.extend_from_slice(&2u16.to_le_bytes());
        hand_g.push(0u8);
        hand_g.extend_from_slice(&5u16.to_le_bytes());
        assert_eq!(g_enc, hand_g, "Op::GroupAggregate wire MUST stay byte-identical");
    }

    // ========================================================================
    // SP-PG-SQL-HAVING — wire KATs for the optional, marker-guarded HAVING
    // block on Op::GroupAggregate (tag 22), Op::GroupAggregateMulti (tag 47),
    // and Op::Join's JoinGroupAgg.
    // ========================================================================

    #[test]
    fn sp_pg_sql_having_wire_round_trip_and_byte_identity() {
        // (1) GroupAggregate with HAVING but EMPTY range_preds: the range-preds
        // length prefix is forced to `0u32` so the HAVING block has a fixed
        // offset, then the HAVING block follows.
        let g = Op::GroupAggregate {
            type_id: 4, program: vec![9], group_field: 2, kind: 0, agg_field: 5,
            range_preds: vec![],
            having: Some(HavingPred { agg_index: 0, op: 1, value: 3 }), sort: None,
        };
        let enc = g.encode();
        let mut hand = Vec::new();
        hand.push(22u8);
        hand.extend_from_slice(&4u32.to_le_bytes());
        hand.extend_from_slice(&1u32.to_le_bytes());
        hand.extend_from_slice(&[9]);
        hand.extend_from_slice(&2u16.to_le_bytes());
        hand.push(0u8);
        hand.extend_from_slice(&5u16.to_le_bytes());
        hand.extend_from_slice(&0u32.to_le_bytes()); // forced empty range_preds len
        hand.push(1u8); // having marker
        hand.extend_from_slice(&0u16.to_le_bytes()); // agg_index
        hand.push(1u8); // op (>=)
        hand.extend_from_slice(&3i128.to_le_bytes()); // value
        assert_eq!(enc, hand, "GroupAggregate+HAVING wire layout");
        assert_eq!(Op::decode(&enc).unwrap(), g, "GroupAggregate+HAVING round-trip");

        // (2) GroupAggregate with HAVING AND non-empty range_preds.
        let g2 = Op::GroupAggregate {
            type_id: 4, program: vec![1], group_field: 1, kind: 1, agg_field: 3,
            range_preds: vec![(2u16, 1u8, vec![1, 0])],
            having: Some(HavingPred { agg_index: 0, op: 0, value: -5 }), sort: None,
        };
        assert_eq!(Op::decode(&g2.encode()).unwrap(), g2, "GA+rp+HAVING round-trip");

        // (3) GroupAggregateMulti with HAVING (agg_index 2 of 3).
        let m = Op::GroupAggregateMulti {
            type_id: 7, program: vec![1, 2], group_field: 1,
            aggregates: vec![(0, 0), (1, 3), (3, 4)],
            range_preds: vec![],
            having: Some(HavingPred { agg_index: 2, op: 3, value: 100 }), sort: None,
        };
        assert_eq!(Op::decode(&m.encode()).unwrap(), m, "GroupAggregateMulti+HAVING round-trip");

        // (4) Join with JoinGroupAgg + HAVING.
        let j = Op::Join {
            left_type: 4, right_type: 5, left_field: 1, right_field: 2,
            limit: 0, filter: vec![], join_type: JoinType::Inner,
            order_by: None, limit_n: None, offset_n: None,
            group_aggregate: Some(JoinGroupAgg {
                group_field: 1,
                aggregates: vec![(0, COUNT_STAR_FIELD)],
                having: Some(HavingPred { agg_index: 0, op: 5, value: 0 }),
            }),
            extra_joins: vec![],
        };
        assert_eq!(Op::decode(&j.encode()).unwrap(), j, "Join+JoinGroupAgg+HAVING round-trip");

        // (5) Byte-identity lock: NO HAVING + no range_preds GroupAggregate
        // encodes to the exact pre-HAVING bytes (8-byte shorter than the
        // HAVING-bearing frame above — no rp-len, no having block).
        let g_none = Op::GroupAggregate {
            type_id: 4, program: vec![9], group_field: 2, kind: 0, agg_field: 5,
            range_preds: vec![], having: None, sort: None,
        };
        let mut hand_none = Vec::new();
        hand_none.push(22u8);
        hand_none.extend_from_slice(&4u32.to_le_bytes());
        hand_none.extend_from_slice(&1u32.to_le_bytes());
        hand_none.extend_from_slice(&[9]);
        hand_none.extend_from_slice(&2u16.to_le_bytes());
        hand_none.push(0u8);
        hand_none.extend_from_slice(&5u16.to_le_bytes());
        assert_eq!(g_none.encode(), hand_none, "no-HAVING GroupAggregate byte-identical");
        let j_none = Op::Join {
            left_type: 4, right_type: 5, left_field: 1, right_field: 2,
            limit: 0, filter: vec![], join_type: JoinType::Inner,
            order_by: None, limit_n: None, offset_n: None,
            group_aggregate: Some(JoinGroupAgg {
                group_field: 1, aggregates: vec![(0, COUNT_STAR_FIELD)], having: None,
            }),
            extra_joins: vec![],
        };
        // The no-HAVING join-group-aggregate must NOT carry a having marker:
        // decode then re-encode must be stable, and the encoded length equals a
        // freshly-built no-HAVING frame (no extra bytes).
        let je = j_none.encode();
        assert_eq!(Op::decode(&je).unwrap(), j_none, "no-HAVING join GA round-trips");
        // Re-encoding the decoded op yields identical bytes (determinism).
        assert_eq!(Op::decode(&je).unwrap().encode(), je, "no-HAVING join GA byte-stable");

        // (6) A non-1 HAVING marker is rejected at decode (forward-incompat).
        let mut bad = hand.clone();
        let mlen = bad.len();
        // overwrite the having marker byte (index after the 0u32 rp-len) with 2
        let marker_idx = mlen - (1 + 2 + 1 + 16);
        bad[marker_idx] = 2;
        assert!(Op::decode(&bad).is_none(), "non-1 HAVING marker rejected");
    }

    /// SP-PG-SQL-GROUP-SORT-LIMIT — wire round-trip + byte-layout + byte-
    /// identity KATs for the new group-sort/page block on `Op::GroupAggregate`
    /// (tag 22) and `Op::GroupAggregateMulti` (tag 47), incl. composition with
    /// HAVING and forward-incompat marker rejection.
    #[test]
    fn sp_pg_sql_group_sort_limit_wire_round_trip_and_byte_identity() {
        // (1) GroupAggregate, NO HAVING + sort by agg index 0 DESC, LIMIT 5
        // OFFSET 1. The range-preds length prefix is forced to `0u32`, then a
        // no-HAVING anchor byte `0`, then the sort block.
        let g = Op::GroupAggregate {
            type_id: 4, program: vec![9], group_field: 2, kind: 0, agg_field: 5,
            range_preds: vec![],
            having: None,
            sort: Some(GroupSort {
                target: GroupSortTarget::Agg(0),
                desc: true,
                limit: Some(5),
                offset: Some(1),
            }),
        };
        let enc = g.encode();
        let mut hand = Vec::new();
        hand.push(22u8);
        hand.extend_from_slice(&4u32.to_le_bytes());
        hand.extend_from_slice(&1u32.to_le_bytes());
        hand.extend_from_slice(&[9]);
        hand.extend_from_slice(&2u16.to_le_bytes());
        hand.push(0u8);
        hand.extend_from_slice(&5u16.to_le_bytes());
        hand.extend_from_slice(&0u32.to_le_bytes()); // forced empty range_preds len
        hand.push(0u8); // no-HAVING anchor
        hand.push(1u8); // group-sort marker
        hand.push(1u8); // target tag 1 = Agg
        hand.extend_from_slice(&0u16.to_le_bytes()); // agg_index 0
        hand.push(1u8); // desc
        hand.push(1u8); // has_limit
        hand.extend_from_slice(&5u64.to_le_bytes());
        hand.push(1u8); // has_offset
        hand.extend_from_slice(&1u64.to_le_bytes());
        assert_eq!(enc, hand, "GroupAggregate+sort wire layout");
        assert_eq!(Op::decode(&enc).unwrap(), g, "GroupAggregate+sort round-trip");

        // (2) GroupAggregate with HAVING AND sort by Key ASC, no LIMIT/OFFSET.
        let g2 = Op::GroupAggregate {
            type_id: 4, program: vec![1], group_field: 1, kind: 1, agg_field: 3,
            range_preds: vec![(2u16, 1u8, vec![1, 0])],
            having: Some(HavingPred { agg_index: 0, op: 0, value: -5 }),
            sort: Some(GroupSort {
                target: GroupSortTarget::Key,
                desc: false,
                limit: None,
                offset: None,
            }),
        };
        assert_eq!(Op::decode(&g2.encode()).unwrap(), g2, "GA+rp+HAVING+sort round-trip");

        // (3) GroupAggregateMulti with HAVING + sort by agg index 2 DESC LIMIT 3.
        let m = Op::GroupAggregateMulti {
            type_id: 7, program: vec![1, 2], group_field: 1,
            aggregates: vec![(0, 0), (1, 3), (3, 4)],
            range_preds: vec![],
            having: Some(HavingPred { agg_index: 2, op: 3, value: 100 }),
            sort: Some(GroupSort {
                target: GroupSortTarget::Agg(2),
                desc: true,
                limit: Some(3),
                offset: None,
            }),
        };
        assert_eq!(Op::decode(&m.encode()).unwrap(), m, "GroupAggregateMulti+HAVING+sort round-trip");

        // (4) GroupAggregateMulti, sort only (no HAVING) — anchor path.
        let m2 = Op::GroupAggregateMulti {
            type_id: 7, program: vec![1], group_field: 1,
            aggregates: vec![(0, 0)],
            range_preds: vec![],
            having: None,
            sort: Some(GroupSort {
                target: GroupSortTarget::Agg(0),
                desc: false,
                limit: None,
                offset: Some(2),
            }),
        };
        assert_eq!(Op::decode(&m2.encode()).unwrap(), m2, "Multi sort-only round-trip");

        // (5) Byte-identity lock: NO HAVING + NO sort GroupAggregate is byte-
        // identical to the pre-arc frame (no rp-len, no anchor, no sort block).
        let g_none = Op::GroupAggregate {
            type_id: 4, program: vec![9], group_field: 2, kind: 0, agg_field: 5,
            range_preds: vec![], having: None, sort: None,
        };
        let mut hand_none = Vec::new();
        hand_none.push(22u8);
        hand_none.extend_from_slice(&4u32.to_le_bytes());
        hand_none.extend_from_slice(&1u32.to_le_bytes());
        hand_none.extend_from_slice(&[9]);
        hand_none.extend_from_slice(&2u16.to_le_bytes());
        hand_none.push(0u8);
        hand_none.extend_from_slice(&5u16.to_le_bytes());
        assert_eq!(g_none.encode(), hand_none, "no-sort/no-HAVING GroupAggregate byte-identical");

        // (6) A non-1 group-sort marker is rejected at decode (forward-incompat).
        let mut bad = hand.clone();
        // The sort marker is the byte right after the no-HAVING anchor: it is
        // the 2nd byte from the rp-len/having region — easiest to find it by
        // walking from the end (layout: marker, tag, u16, desc, 1, u64, 1, u64).
        let sort_marker_idx = bad.len() - (1 + 1 + 2 + 1 + 1 + 8 + 1 + 8);
        assert_eq!(bad[sort_marker_idx], 1u8, "located the sort marker");
        bad[sort_marker_idx] = 2;
        assert!(Op::decode(&bad).is_none(), "non-1 group-sort marker rejected");

        // (7) An out-of-range target tag (not 0/1) is rejected at decode.
        let mut bad2 = hand.clone();
        bad2[sort_marker_idx + 1] = 9; // target tag byte
        assert!(Op::decode(&bad2).is_none(), "bad group-sort target tag rejected");
    }
}
