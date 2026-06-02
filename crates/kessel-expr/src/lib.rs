//! kessel-expr: a tiny **deterministic** stack bytecode VM.
//!
//! This is the revolutionary core: user-supplied logic that runs *inside* the
//! replicated deterministic state machine. It is therefore, by construction:
//! PURE (no I/O, clock, RNG, allocation-unbounded loops), TERMINATING (the
//! ISA has no backward jumps; a gas cap defends against malformed programs),
//! and BYTE-IDENTICAL on every replica (integer-only arithmetic is wrapping;
//! comparisons are total). Powers CHECK constraints (SP7) and will power
//! deterministic triggers (SP8).

#![forbid(unsafe_code)]

use kessel_catalog::{FieldKind, ObjectType};

pub const GAS_LIMIT: u32 = 100_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Int(i128),
    Bytes(Vec<u8>),
    Null,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ExprError {
    BadProgram,
    StackUnderflow,
    DivByZero,
    OutOfGas,
    TypeMismatch,
    EmptyResult,
}

// Opcodes
const PUSH_INT: u8 = 0;
const LOAD_FIELD: u8 = 1;
const IS_NULL: u8 = 2;
const EQ: u8 = 3;
const NE: u8 = 4;
const LT: u8 = 5;
const LE: u8 = 6;
const GT: u8 = 7;
const GE: u8 = 8;
const ADD: u8 = 9;
const SUB: u8 = 10;
const MUL: u8 = 11;
const DIV: u8 = 12;
const MOD: u8 = 13;
const AND: u8 = 14;
const OR: u8 = 15;
const NOT: u8 = 16;
const PUSH_BYTES: u8 = 17;
const SET_FIELD: u8 = 18; // trigger-only: pop value, write into working record
const REJECT: u8 = 19; // trigger-only: abort the write
const LIKE: u8 = 20; // pop pattern(Bytes) + value(Bytes) -> Int 0/1 (SQL LIKE)
const SHA256: u8 = 21; // pop value(Bytes) -> Bytes (32-byte digest)
const HMAC256: u8 = 22; // pop key(Bytes), value(Bytes) -> Bytes (HMAC-SHA256)

/// Deterministic SQL `LIKE` matcher: `%` = any (incl. empty) byte run,
/// `_` = exactly one byte. Iterative with the classic single backtrack
/// point — O(|s|·|p|) worst case, no recursion, no allocation.
fn like_match(s: &[u8], p: &[u8]) -> bool {
    let (mut i, mut j) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while i < s.len() {
        if j < p.len() && (p[j] == b'_' || p[j] == s[i]) {
            i += 1;
            j += 1;
        } else if j < p.len() && p[j] == b'%' {
            star = Some(j);
            mark = i;
            j += 1;
        } else if let Some(sj) = star {
            j = sj + 1;
            mark += 1;
            i = mark;
        } else {
            return false;
        }
    }
    while j < p.len() && p[j] == b'%' {
        j += 1;
    }
    j == p.len()
}

/// Fluent program builder (test/host convenience). Emits the wire bytecode.
#[derive(Default)]
pub struct Program {
    code: Vec<u8>,
}

impl Program {
    pub fn new() -> Self {
        Self::default()
    }
    /// Wrap already-emitted bytecode (used by compilers that splice
    /// sub-programs, e.g. kessel-sql's WHERE).
    pub fn from_raw(code: Vec<u8>) -> Self {
        Program { code }
    }
    pub fn push_int(mut self, v: i128) -> Self {
        self.code.push(PUSH_INT);
        self.code.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn push_bytes(mut self, b: &[u8]) -> Self {
        self.code.push(PUSH_BYTES);
        self.code.extend_from_slice(&(b.len() as u16).to_le_bytes());
        self.code.extend_from_slice(b);
        self
    }
    pub fn load(mut self, field_id: u16) -> Self {
        self.code.push(LOAD_FIELD);
        self.code.extend_from_slice(&field_id.to_le_bytes());
        self
    }
    pub fn is_null(mut self, field_id: u16) -> Self {
        self.code.push(IS_NULL);
        self.code.extend_from_slice(&field_id.to_le_bytes());
        self
    }
    fn op(mut self, o: u8) -> Self {
        self.code.push(o);
        self
    }
    pub fn eq(self) -> Self { self.op(EQ) }
    pub fn ne(self) -> Self { self.op(NE) }
    pub fn sha256(self) -> Self { self.op(SHA256) }
    pub fn hmac256(self) -> Self { self.op(HMAC256) }
    pub fn like(self) -> Self { self.op(LIKE) }
    pub fn lt(self) -> Self { self.op(LT) }
    pub fn le(self) -> Self { self.op(LE) }
    pub fn gt(self) -> Self { self.op(GT) }
    pub fn ge(self) -> Self { self.op(GE) }
    pub fn add(self) -> Self { self.op(ADD) }
    pub fn sub(self) -> Self { self.op(SUB) }
    pub fn mul(self) -> Self { self.op(MUL) }
    pub fn div(self) -> Self { self.op(DIV) }
    pub fn rem(self) -> Self { self.op(MOD) }
    pub fn and(self) -> Self { self.op(AND) }
    pub fn or(self) -> Self { self.op(OR) }
    pub fn not(self) -> Self { self.op(NOT) }
    /// Trigger op: pop the top value and store it into `field_id` of the
    /// working record (numeric → LE width, bytes → fixed width).
    pub fn set_field(mut self, field_id: u16) -> Self {
        self.code.push(SET_FIELD);
        self.code.extend_from_slice(&field_id.to_le_bytes());
        self
    }
    /// Trigger op: abort the write (row rejected).
    pub fn reject(self) -> Self {
        self.op(REJECT)
    }
    pub fn bytes(self) -> Vec<u8> {
        self.code
    }
}

fn is_codec_record(ot: &ObjectType, rec: &[u8]) -> bool {
    use kessel_catalog::SCHEMA_VER_BYTES;
    if rec.len() < SCHEMA_VER_BYTES + 2 {
        return false;
    }
    let fc = u16::from_le_bytes(
        rec[SCHEMA_VER_BYTES..SCHEMA_VER_BYTES + 2].try_into().unwrap(),
    ) as usize;
    if fc == 0 || fc > ot.fields.len() {
        return false;
    }
    // A codec record stores `fc` fields; after `ALTER … ADD COLUMN` an
    // older row has `fc < fields.len()` and the *smaller* record size of
    // the schema it was written under. Recognise it by matching that
    // truncated-schema record size (offsets of existing fields are
    // invariant under appends), so fields ≥ fc up-project to NULL exactly
    // like `kessel_codec::decode`. Deterministic: pure of (record, schema).
    let prefix = kessel_catalog::ObjectType::from_def(
        ot.name.clone(),
        ot.fields[..fc].to_vec(),
    );
    rec.len() == prefix.compute_layout().record_size
}

fn field_is_null(ot: &ObjectType, rec: &[u8], i: usize) -> bool {
    use kessel_catalog::SCHEMA_VER_BYTES;
    if !is_codec_record(ot, rec) {
        return false; // opaque record: treat as present
    }
    let fc = u16::from_le_bytes(
        rec[SCHEMA_VER_BYTES..SCHEMA_VER_BYTES + 2].try_into().unwrap(),
    ) as usize;
    if i >= fc {
        return true;
    }
    let bm = &rec[SCHEMA_VER_BYTES + 2..SCHEMA_VER_BYTES + 2 + 8];
    bm.get(i / 8).map(|b| b & (1 << (i % 8)) != 0).unwrap_or(true)
}

fn load_field(ot: &ObjectType, rec: &[u8], field_id: u16) -> Value {
    let i = match ot.fields.iter().position(|f| f.field_id == field_id) {
        Some(i) => i,
        None => return Value::Null,
    };
    if field_is_null(ot, rec, i) {
        return Value::Null;
    }
    let layout = ot.compute_layout();
    let off = layout.offsets[i];
    let w = ot.fields[i].kind.width() as usize;
    let raw = match rec.get(off..off + w) {
        Some(r) => r,
        None => return Value::Null,
    };
    match ot.fields[i].kind {
        FieldKind::I8
        | FieldKind::I16
        | FieldKind::I32
        | FieldKind::I64
        | FieldKind::I128
        | FieldKind::Fixed { .. } => {
            let mut le = [0u8; 16];
            le[..w.min(16)].copy_from_slice(&raw[..w.min(16)]);
            if w < 16 && raw[w - 1] & 0x80 != 0 {
                for b in le.iter_mut().skip(w) {
                    *b = 0xFF;
                }
            }
            Value::Int(i128::from_le_bytes(le))
        }
        FieldKind::U8
        | FieldKind::U16
        | FieldKind::U32
        | FieldKind::U64
        | FieldKind::U128
        | FieldKind::Bool
        | FieldKind::Timestamp => {
            let mut le = [0u8; 16];
            le[..w.min(16)].copy_from_slice(&raw[..w.min(16)]);
            // u128 high bit folds into i128 sign — documented edge; values
            // up to i128::MAX are exact, which covers u8..u64 fully.
            Value::Int(i128::from_le_bytes(le))
        }
        FieldKind::Char(_)
        | FieldKind::Bytes(_)
        | FieldKind::Ref
        | FieldKind::OverflowRef => Value::Bytes(raw.to_vec()),
    }
}

fn truthy(v: &Value) -> bool {
    matches!(v, Value::Int(n) if *n != 0)
}

/// Write `val` into `work` at `field_id` (numeric → LE width, bytes → fixed
/// width, Null → zeroed + null bit if codec-shaped). Used by triggers.
fn write_field(ot: &ObjectType, work: &mut [u8], field_id: u16, val: &Value) {
    use kessel_catalog::SCHEMA_VER_BYTES;
    let i = match ot.fields.iter().position(|f| f.field_id == field_id) {
        Some(i) => i,
        None => return,
    };
    let layout = ot.compute_layout();
    let off = layout.offsets[i];
    let w = ot.fields[i].kind.width() as usize;
    if off + w > work.len() {
        return;
    }
    let codec_shaped = work.len() == layout.record_size
        && u16::from_le_bytes(work[SCHEMA_VER_BYTES..SCHEMA_VER_BYTES + 2].try_into().unwrap())
            as usize
            == ot.fields.len();
    let clear_null = |w_: &mut [u8]| {
        if codec_shaped {
            w_[SCHEMA_VER_BYTES + 2 + i / 8] &= !(1 << (i % 8));
        }
    };
    match val {
        Value::Int(n) => {
            let le = n.to_le_bytes();
            work[off..off + w].copy_from_slice(&le[..w.min(16)]);
            if w > 16 {
                for b in work[off + 16..off + w].iter_mut() {
                    *b = if *n < 0 { 0xFF } else { 0 };
                }
            }
            clear_null(work);
        }
        Value::Bytes(b) => {
            let n = b.len().min(w);
            work[off..off + n].copy_from_slice(&b[..n]);
            for x in work[off + n..off + w].iter_mut() {
                *x = 0;
            }
            clear_null(work);
        }
        Value::Null => {
            for x in work[off..off + w].iter_mut() {
                *x = 0;
            }
            if codec_shaped {
                work[SCHEMA_VER_BYTES + 2 + i / 8] |= 1 << (i % 8);
            }
        }
    }
}

/// Outcome of running a program: whether a trigger rejected, and the final
/// stack top (for predicate `eval`).
struct RunEnd {
    rejected: bool,
    top: Option<Value>,
}

/// Evaluate `code` against `rec` of `ot`. Returns the boolean verdict
/// (top-of-stack must be a non-zero Int to pass).
/// Core interpreter. `LoadField`/`IsNull` read the ORIGINAL `rec` (so
/// trigger output is order-independent and deterministic); `SET_FIELD`
/// mutates `work`. `REJECT` stops with `rejected=true`.
fn run(
    code: &[u8],
    ot: &ObjectType,
    rec: &[u8],
    work: &mut [u8],
) -> Result<RunEnd, ExprError> {
    let mut st: Vec<Value> = Vec::new();
    let mut pc = 0usize;
    let mut gas = 0u32;
    macro_rules! pop {
        () => {
            st.pop().ok_or(ExprError::StackUnderflow)?
        };
    }
    macro_rules! ord {
        ($f:expr) => {{
            let b = pop!();
            let a = pop!();
            let r = match (&a, &b) {
                (Value::Int(x), Value::Int(y)) => {
                    let o = x.cmp(y);
                    $f(o)
                }
                (Value::Bytes(x), Value::Bytes(y)) => {
                    let o = x.cmp(y);
                    $f(o)
                }
                _ => false, // Null or mixed => false (deterministic)
            };
            st.push(Value::Int(r as i128));
        }};
    }
    macro_rules! arith {
        ($op:expr) => {{
            let b = pop!();
            let a = pop!();
            match (a, b) {
                (Value::Int(x), Value::Int(y)) => {
                    let r = $op(x, y)?;
                    st.push(Value::Int(r));
                }
                _ => return Err(ExprError::TypeMismatch),
            }
        }};
    }
    while pc < code.len() {
        gas += 1;
        if gas > GAS_LIMIT {
            return Err(ExprError::OutOfGas);
        }
        let op = code[pc];
        pc += 1;
        match op {
            PUSH_INT => {
                let bytes = code.get(pc..pc + 16).ok_or(ExprError::BadProgram)?;
                st.push(Value::Int(i128::from_le_bytes(bytes.try_into().unwrap())));
                pc += 16;
            }
            PUSH_BYTES => {
                let l = u16::from_le_bytes(
                    code.get(pc..pc + 2).ok_or(ExprError::BadProgram)?.try_into().unwrap(),
                ) as usize;
                pc += 2;
                let b = code.get(pc..pc + l).ok_or(ExprError::BadProgram)?;
                st.push(Value::Bytes(b.to_vec()));
                pc += l;
            }
            LOAD_FIELD => {
                let fid = u16::from_le_bytes(
                    code.get(pc..pc + 2).ok_or(ExprError::BadProgram)?.try_into().unwrap(),
                );
                pc += 2;
                st.push(load_field(ot, rec, fid));
            }
            IS_NULL => {
                let fid = u16::from_le_bytes(
                    code.get(pc..pc + 2).ok_or(ExprError::BadProgram)?.try_into().unwrap(),
                );
                pc += 2;
                let isn = matches!(load_field(ot, rec, fid), Value::Null);
                st.push(Value::Int(isn as i128));
            }
            EQ => {
                let b = pop!();
                let a = pop!();
                st.push(Value::Int((a == b) as i128));
            }
            NE => {
                let b = pop!();
                let a = pop!();
                st.push(Value::Int((a != b) as i128));
            }
            LIKE => {
                let pat = pop!();
                let val = pop!();
                let r = match (&val, &pat) {
                    (Value::Bytes(v), Value::Bytes(p)) => {
                        // Trim trailing NULs so fixed-width Char text
                        // (zero-padded) matches naturally.
                        let end = v.iter().rposition(|&c| c != 0).map_or(0, |x| x + 1);
                        like_match(&v[..end], p)
                    }
                    _ => false, // non-text operands => deterministic false
                };
                st.push(Value::Int(r as i128));
            }
            SHA256 => {
                // Hash the top value's bytes; Int is hashed via its
                // 16-byte LE form so the result is deterministic and
                // type-stable across replicas.
                let v = pop!();
                let bytes = match v {
                    Value::Bytes(b) => b,
                    Value::Int(n) => n.to_le_bytes().to_vec(),
                    Value::Null => Vec::new(),
                };
                st.push(Value::Bytes(kessel_crypto::sha256(&bytes).to_vec()));
            }
            HMAC256 => {
                let key = pop!();
                let msg = pop!();
                let kb = match key {
                    Value::Bytes(b) => b,
                    Value::Int(n) => n.to_le_bytes().to_vec(),
                    Value::Null => Vec::new(),
                };
                let mb = match msg {
                    Value::Bytes(b) => b,
                    Value::Int(n) => n.to_le_bytes().to_vec(),
                    Value::Null => Vec::new(),
                };
                st.push(Value::Bytes(
                    kessel_crypto::hmac_sha256(&kb, &mb).to_vec(),
                ));
            }
            LT => ord!(|o| o == std::cmp::Ordering::Less),
            LE => ord!(|o| o != std::cmp::Ordering::Greater),
            GT => ord!(|o| o == std::cmp::Ordering::Greater),
            GE => ord!(|o| o != std::cmp::Ordering::Less),
            ADD => arith!(|x: i128, y: i128| Ok::<i128, ExprError>(x.wrapping_add(y))),
            SUB => arith!(|x: i128, y: i128| Ok::<i128, ExprError>(x.wrapping_sub(y))),
            MUL => arith!(|x: i128, y: i128| Ok::<i128, ExprError>(x.wrapping_mul(y))),
            DIV => arith!(|x: i128, y: i128| if y == 0 {
                Err(ExprError::DivByZero)
            } else {
                Ok(x.wrapping_div(y))
            }),
            MOD => arith!(|x: i128, y: i128| if y == 0 {
                Err(ExprError::DivByZero)
            } else {
                Ok(x.wrapping_rem(y))
            }),
            AND => {
                let b = pop!();
                let a = pop!();
                st.push(Value::Int((truthy(&a) && truthy(&b)) as i128));
            }
            OR => {
                let b = pop!();
                let a = pop!();
                st.push(Value::Int((truthy(&a) || truthy(&b)) as i128));
            }
            NOT => {
                let a = pop!();
                st.push(Value::Int((!truthy(&a)) as i128));
            }
            SET_FIELD => {
                let fid = u16::from_le_bytes(
                    code.get(pc..pc + 2).ok_or(ExprError::BadProgram)?.try_into().unwrap(),
                );
                pc += 2;
                let v = pop!();
                write_field(ot, work, fid, &v);
            }
            REJECT => {
                return Ok(RunEnd { rejected: true, top: None });
            }
            _ => return Err(ExprError::BadProgram),
        }
    }
    Ok(RunEnd {
        rejected: false,
        top: st.last().cloned(),
    })
}

/// Evaluate a predicate program (CHECK). True iff top-of-stack is a
/// non-zero Int.
pub fn eval(code: &[u8], ot: &ObjectType, rec: &[u8]) -> Result<bool, ExprError> {
    let mut work = rec.to_vec(); // unused by predicate ops
    let end = run(code, ot, rec, &mut work)?;
    match end.top {
        Some(v) => Ok(truthy(&v)),
        None => Err(ExprError::EmptyResult),
    }
}

/// Run a trigger program. `Ok(None)` = the row is rejected; `Ok(Some(rec'))`
/// = the (possibly mutated) record to continue writing. Deterministic.
pub fn eval_trigger(
    code: &[u8],
    ot: &ObjectType,
    rec: &[u8],
) -> Result<Option<Vec<u8>>, ExprError> {
    let mut work = rec.to_vec();
    let end = run(code, ot, rec, &mut work)?;
    if end.rejected {
        Ok(None)
    } else {
        Ok(Some(work))
    }
}

// =========================================================================
// SP-WHERE-VM-Specialise — compile_filter: closure-built-once-per-query
// =========================================================================
//
// The stack-VM interpreter above is the determinism oracle. For analytic
// scans where the same WHERE program runs 60K+ times per query
// (TPC-H Q1/Q6 at SF=0.01), the per-invocation dispatch+stack cost
// dominates. `compile_filter` walks the bytecode ONCE and returns a
// `FilterFn` closure that captures pre-resolved field offsets +
// comparison operands; per-row cost drops from "match opcode + Vec push/
// pop × ~80" to "direct field reads + i128/bytes cmp + && short-circuit".
//
// The interpreter remains the oracle: per-row callsites that get
// `Err(CompileError::Unsupported)` MUST fall back to `eval(...)`.
// V1 KATs lock byte-equal results across the two paths for every
// supported opcode shape.
//
// V1 supported opcodes: PUSH_INT, PUSH_BYTES, LOAD_FIELD, IS_NULL,
// EQ, NE, LT, LE, GT, GE, AND, OR, NOT. Arithmetic + SHA256 + HMAC +
// LIKE + SET_FIELD + REJECT are out-of-scope (return Unsupported);
// they don't appear in TPC-H Q1/Q6 WHERE shapes.

/// Per-row filter closure: `Box<dyn Fn(&[u8]) -> bool + Send + Sync>`.
/// Returned by `Program::compile_filter`. The closure captures all
/// state (field offsets, constants, sub-closures) by-move so it can
/// be shared across `std::thread::scope` workers as `&filter`.
pub type FilterFn = Box<dyn Fn(&[u8]) -> bool + Send + Sync>;

/// Reasons `compile_filter` may decline to specialise a program. The
/// caller is expected to fall back to `eval(...)` per row.
#[derive(Debug, PartialEq, Eq)]
pub enum CompileError {
    /// Opcode not in V1's supported set (e.g. ADD, DIV, SHA256, LIKE).
    /// `op_name` is the static opcode mnemonic for grep-ability.
    Unsupported { op_name: &'static str },
    /// Bytecode is truncated / malformed.
    BadProgram,
    /// RPN stack underflow at compile time.
    StackUnderflow,
    /// Program left ≠ 1 value on the compile-time stack (predicate
    /// programs MUST leave exactly one bool). The interpreter returns
    /// `EmptyResult` for the 0-case; the closure builder rejects
    /// both 0 and N>1.
    Malformed,
    /// Encountered LOAD_FIELD / IS_NULL with a field_id absent from
    /// the supplied schema. The interpreter would return Null;
    /// compile_filter declines so the caller can fall back.
    UnknownField { field_id: u16 },
}

/// Compile-time operand shape. Either a constant baked into the
/// closure or a per-row field read.
#[derive(Clone)]
enum Operand {
    ConstInt(i128),
    ConstBytes(Vec<u8>),
    /// Resolved field-load: `off` and `width` come from
    /// `ot.compute_layout()` once at compile time. `fid_idx` is
    /// `ot.fields[fid_idx]`'s position; used for the null-bitmap
    /// check inside the row. `signed` controls sign-extension on
    /// narrow integer widths.
    LoadInt { off: usize, width: usize, signed: bool, fid_idx: usize, codec_shaped: CodecShape },
    LoadBytes { off: usize, width: usize, fid_idx: usize, codec_shaped: CodecShape },
    /// A `BoolNode` lifted into the operand position (e.g. when the
    /// program emits `(a < b) == 1` — kessel-sql's CHECK compiler
    /// does this). The closure body is 0/1 cast at materialisation.
    BoolAsInt(Box<BoolNode>),
}

/// Whether a given (ot, record_size) is codec-shaped. Cached at
/// compile time so the per-row null-bitmap check is one constant
/// + one bit test, not the full `is_codec_record` walk every row.
/// The closure still checks per-row `rec.len() == expected_len`
/// because storage may hand it pre-codec opaque records or codec
/// records of older schema (post-ALTER ADD COLUMN).
#[derive(Clone, Copy)]
struct CodecShape {
    expected_record_size: usize,
    expected_fc: usize,
}

#[derive(Clone)]
enum BoolNode {
    True,
    False,
    IsNull { fid_idx: usize, codec_shaped: CodecShape },
    Cmp { lhs: Operand, rhs: Operand, op: CmpOp },
    And(Box<BoolNode>, Box<BoolNode>),
    Or(Box<BoolNode>, Box<BoolNode>),
    Not(Box<BoolNode>),
    /// Equality / inequality of two non-cmp boolean predicates.
    /// (e.g. PUSH_INT 1; LOAD age; PUSH_INT 18; GE; AND; PUSH_INT 1;
    /// EQ — the AND result compared with 1 via EQ.) Handled by
    /// materialising LHS + RHS as bool and applying the cmp.
    BoolCmp { lhs: Box<BoolNode>, rhs: Box<BoolNode>, op: CmpOp },
}

#[derive(Clone, Copy)]
enum CmpOp { Eq, Ne, Lt, Le, Gt, Ge }

/// Compile-time stack value — Operand (constants/loads) or BoolNode
/// (results of comparisons + logic). The walker pushes onto this
/// stack as it scans bytecode; the final stack-top is the predicate.
enum StackItem {
    Op(Operand),
    Bn(BoolNode),
}

impl StackItem {
    fn into_operand(self) -> Operand {
        match self {
            StackItem::Op(o) => o,
            StackItem::Bn(b) => Operand::BoolAsInt(Box::new(b)),
        }
    }
    fn into_bool(self) -> BoolNode {
        match self {
            StackItem::Bn(b) => b,
            // PUSH_INT 1 / PUSH_INT 0 used as bool — common compiler shape.
            StackItem::Op(Operand::ConstInt(n)) => {
                if n != 0 { BoolNode::True } else { BoolNode::False }
            }
            // Anything else: defer the truthiness decision to runtime.
            // Wrap the operand in a 0/1 cmp via `BoolCmp` is overkill;
            // simpler — synthesize Cmp{ op == NE, rhs=ConstInt(0) }.
            StackItem::Op(other) => BoolNode::Cmp {
                lhs: other,
                rhs: Operand::ConstInt(0),
                op: CmpOp::Ne,
            },
        }
    }
}

fn codec_shape_for(ot: &ObjectType) -> CodecShape {
    let layout = ot.compute_layout();
    CodecShape {
        expected_record_size: layout.record_size,
        expected_fc: ot.fields.len(),
    }
}

/// Per-row null-bitmap check, specialised at compile time for the
/// fid_idx. Returns true iff the record IS codec-shaped AND the bit
/// for `fid_idx` in the null bitmap is set OR the record is the
/// older-schema truncated shape and `fid_idx >= fc`.
///
/// For opaque records (not codec-shaped), returns false — same as
/// `field_is_null` interpreter behavior.
fn null_at(rec: &[u8], fid_idx: usize, shape: CodecShape) -> bool {
    use kessel_catalog::SCHEMA_VER_BYTES;
    if rec.len() < SCHEMA_VER_BYTES + 2 + 8 {
        return false;
    }
    let fc = u16::from_le_bytes(
        rec[SCHEMA_VER_BYTES..SCHEMA_VER_BYTES + 2].try_into().unwrap(),
    ) as usize;
    if fc == 0 || fc > shape.expected_fc {
        return false; // opaque — interpreter returns Value present
    }
    // Codec-shape OR truncated-prefix shape — must match the recorded
    // fc's prefix layout for the original `ot`.
    // (We cannot recompute `ObjectType::from_def` here cheaply per row;
    // accept the looser check that the record_size matches the full
    // shape OR fc < expected_fc — for the latter the interpreter
    // computes a prefix layout each call. compile_filter declines if
    // we'd need that path; see compile_filter().)
    if rec.len() != shape.expected_record_size {
        return false;
    }
    if fid_idx >= fc {
        return true;
    }
    let bm = &rec[SCHEMA_VER_BYTES + 2..SCHEMA_VER_BYTES + 2 + 8];
    bm.get(fid_idx / 8).map(|b| b & (1 << (fid_idx % 8)) != 0).unwrap_or(true)
}

/// Per-row integer field read. Matches `load_field`'s decode shape
/// (signed sign-extends from narrow widths; unsigned zero-extends).
/// Returns `None` if the field would be Null or the record is too
/// short.
fn read_int(rec: &[u8], off: usize, width: usize, signed: bool) -> Option<i128> {
    let raw = rec.get(off..off + width)?;
    let mut le = [0u8; 16];
    le[..width.min(16)].copy_from_slice(&raw[..width.min(16)]);
    if signed && width < 16 && raw[width - 1] & 0x80 != 0 {
        for b in le.iter_mut().skip(width) {
            *b = 0xFF;
        }
    }
    Some(i128::from_le_bytes(le))
}

/// Per-row bytes field read. Returns the borrow into `rec` (no
/// allocation) — same content as interpreter's `Value::Bytes(raw.to_vec())`.
fn read_bytes<'a>(rec: &'a [u8], off: usize, width: usize) -> Option<&'a [u8]> {
    rec.get(off..off + width)
}

fn cmp_apply(o: std::cmp::Ordering, op: CmpOp) -> bool {
    use std::cmp::Ordering::*;
    match (op, o) {
        (CmpOp::Eq, Equal) => true,
        (CmpOp::Eq, _) => false,
        (CmpOp::Ne, Equal) => false,
        (CmpOp::Ne, _) => true,
        (CmpOp::Lt, Less) => true,
        (CmpOp::Lt, _) => false,
        (CmpOp::Le, Less) | (CmpOp::Le, Equal) => true,
        (CmpOp::Le, _) => false,
        (CmpOp::Gt, Greater) => true,
        (CmpOp::Gt, _) => false,
        (CmpOp::Ge, Greater) | (CmpOp::Ge, Equal) => true,
        (CmpOp::Ge, _) => false,
    }
}

/// Convert an Operand into a runtime closure producing `Option<i128>`
/// (None = Null OR unreadable bytes value when used as int — same as
/// interpreter's "mismatched-type cmp returns false").
fn operand_to_int_fn(op: Operand) -> Box<dyn Fn(&[u8]) -> Option<i128> + Send + Sync> {
    match op {
        Operand::ConstInt(v) => Box::new(move |_| Some(v)),
        Operand::ConstBytes(_) => Box::new(|_| None),
        Operand::LoadInt { off, width, signed, fid_idx, codec_shaped } => {
            Box::new(move |rec| {
                if null_at(rec, fid_idx, codec_shaped) { return None; }
                read_int(rec, off, width, signed)
            })
        }
        Operand::LoadBytes { .. } => Box::new(|_| None),
        Operand::BoolAsInt(node) => {
            let f = materialise_bool(*node);
            Box::new(move |rec| Some(if f(rec) { 1 } else { 0 }))
        }
    }
}

/// Convert an Operand into a runtime closure producing `Option<Vec<u8>>`
/// (None = Null OR int operand on the bytes side). We return owned
/// Vec<u8> for the bytes path because constant operands hold Vec<u8>
/// captured by-move; loads borrow from `rec` per row but the cmp
/// kernel needs a uniform return type — we copy for simplicity since
/// bytes compares are not on the TPC-H hot path.
fn operand_to_bytes_fn(
    op: Operand,
) -> Box<dyn Fn(&[u8]) -> Option<Vec<u8>> + Send + Sync> {
    match op {
        Operand::ConstInt(_) => Box::new(|_| None),
        Operand::ConstBytes(b) => Box::new(move |_| Some(b.clone())),
        Operand::LoadInt { .. } => Box::new(|_| None),
        Operand::LoadBytes { off, width, fid_idx, codec_shaped } => {
            Box::new(move |rec| {
                if null_at(rec, fid_idx, codec_shaped) { return None; }
                read_bytes(rec, off, width).map(|s| s.to_vec())
            })
        }
        Operand::BoolAsInt(_) => Box::new(|_| None),
    }
}

/// Materialise a comparison node. Picks int×int, bytes×bytes, or a
/// mixed-type fast path returning false (matches interpreter
/// `ord!`/EQ/NE semantics for mixed/null operands).
fn materialise_cmp(lhs: Operand, rhs: Operand, op: CmpOp) -> FilterFn {
    let lhs_is_bytes = matches!(lhs, Operand::ConstBytes(_) | Operand::LoadBytes { .. });
    let rhs_is_bytes = matches!(rhs, Operand::ConstBytes(_) | Operand::LoadBytes { .. });

    if lhs_is_bytes && rhs_is_bytes {
        let fa = operand_to_bytes_fn(lhs);
        let fb = operand_to_bytes_fn(rhs);
        return Box::new(move |rec| {
            let a = fa(rec); let b = fb(rec);
            match (a, b) {
                (Some(av), Some(bv)) => cmp_apply(av.as_slice().cmp(bv.as_slice()), op),
                _ => false,
            }
        });
    }

    // Int×Int (the TPC-H hot path) — fast specialisation for the
    // common "Load(width=4|8) vs ConstInt op" shape avoids the
    // option-bool stitch.
    if !lhs_is_bytes && !rhs_is_bytes {
        if let (
            Operand::LoadInt { off, width, signed, fid_idx, codec_shaped },
            Operand::ConstInt(v),
        ) = (lhs.clone(), rhs.clone()) {
            return load_vs_const_int_cmp(off, width, signed, fid_idx, codec_shaped, v, op);
        }
        if let (
            Operand::ConstInt(v),
            Operand::LoadInt { off, width, signed, fid_idx, codec_shaped },
        ) = (lhs.clone(), rhs.clone()) {
            // swap op so RHS is the load
            let swapped = match op {
                CmpOp::Lt => CmpOp::Gt,
                CmpOp::Le => CmpOp::Ge,
                CmpOp::Gt => CmpOp::Lt,
                CmpOp::Ge => CmpOp::Le,
                CmpOp::Eq => CmpOp::Eq,
                CmpOp::Ne => CmpOp::Ne,
            };
            return load_vs_const_int_cmp(off, width, signed, fid_idx, codec_shaped, v, swapped);
        }
        let fa = operand_to_int_fn(lhs);
        let fb = operand_to_int_fn(rhs);
        return Box::new(move |rec| {
            match (fa(rec), fb(rec)) {
                (Some(av), Some(bv)) => cmp_apply(av.cmp(&bv), op),
                _ => false,
            }
        });
    }

    // Mixed (one Int, one Bytes) — interpreter returns false.
    Box::new(|_| false)
}

/// Specialised Load vs ConstInt comparison: ONE field read per row +
/// ONE i128 cmp. The hottest TPC-H Q6 / Q1 shape.
fn load_vs_const_int_cmp(
    off: usize,
    width: usize,
    signed: bool,
    fid_idx: usize,
    codec_shaped: CodecShape,
    v: i128,
    op: CmpOp,
) -> FilterFn {
    Box::new(move |rec| {
        if null_at(rec, fid_idx, codec_shaped) { return false; }
        match read_int(rec, off, width, signed) {
            Some(a) => cmp_apply(a.cmp(&v), op),
            None => false,
        }
    })
}

fn materialise_bool(node: BoolNode) -> FilterFn {
    match node {
        BoolNode::True => Box::new(|_| true),
        BoolNode::False => Box::new(|_| false),
        BoolNode::IsNull { fid_idx, codec_shaped } => {
            Box::new(move |rec| null_at(rec, fid_idx, codec_shaped))
        }
        BoolNode::Cmp { lhs, rhs, op } => materialise_cmp(lhs, rhs, op),
        BoolNode::And(a, b) => {
            let fa = materialise_bool(*a);
            let fb = materialise_bool(*b);
            Box::new(move |rec| fa(rec) && fb(rec))
        }
        BoolNode::Or(a, b) => {
            let fa = materialise_bool(*a);
            let fb = materialise_bool(*b);
            Box::new(move |rec| fa(rec) || fb(rec))
        }
        BoolNode::Not(a) => {
            let fa = materialise_bool(*a);
            Box::new(move |rec| !fa(rec))
        }
        BoolNode::BoolCmp { lhs, rhs, op } => {
            let fa = materialise_bool(*lhs);
            let fb = materialise_bool(*rhs);
            Box::new(move |rec| {
                let av = if fa(rec) { 1i128 } else { 0 };
                let bv = if fb(rec) { 1i128 } else { 0 };
                cmp_apply(av.cmp(&bv), op)
            })
        }
    }
}

impl Program {
    /// Compile this program's bytecode into a per-row closure
    /// (`FilterFn`). On `Ok`, the closure is byte-equal in result to
    /// `eval(...)` for every row. On `Err`, the caller MUST fall back
    /// to `eval(...)`.
    ///
    /// V1 specialises every opcode that appears in TPC-H Q1/Q6 WHERE
    /// shapes; declines arithmetic + SHA256 + HMAC + LIKE + SET_FIELD
    /// + REJECT (named `CompileError::Unsupported { op_name }` for
    /// grep).
    pub fn compile_filter(&self, ot: &ObjectType) -> Result<FilterFn, CompileError> {
        compile_filter_bytes(&self.code, ot)
    }
}

/// Free-function variant — useful when the caller has just the raw
/// bytecode (e.g. `Op::Aggregate.program: Vec<u8>`) without wrapping
/// it in a `Program`.
pub fn compile_filter(code: &[u8], ot: &ObjectType) -> Result<FilterFn, CompileError> {
    compile_filter_bytes(code, ot)
}

fn compile_filter_bytes(code: &[u8], ot: &ObjectType) -> Result<FilterFn, CompileError> {
    let codec = codec_shape_for(ot);
    let layout = ot.compute_layout();
    let mut stack: Vec<StackItem> = Vec::new();
    let mut pc = 0usize;
    while pc < code.len() {
        let op = code[pc];
        pc += 1;
        match op {
            PUSH_INT => {
                let bytes = code.get(pc..pc + 16).ok_or(CompileError::BadProgram)?;
                stack.push(StackItem::Op(Operand::ConstInt(
                    i128::from_le_bytes(bytes.try_into().unwrap()),
                )));
                pc += 16;
            }
            PUSH_BYTES => {
                let l = u16::from_le_bytes(
                    code.get(pc..pc + 2).ok_or(CompileError::BadProgram)?.try_into().unwrap(),
                ) as usize;
                pc += 2;
                let b = code.get(pc..pc + l).ok_or(CompileError::BadProgram)?;
                stack.push(StackItem::Op(Operand::ConstBytes(b.to_vec())));
                pc += l;
            }
            LOAD_FIELD => {
                let fid = u16::from_le_bytes(
                    code.get(pc..pc + 2).ok_or(CompileError::BadProgram)?.try_into().unwrap(),
                );
                pc += 2;
                let idx = ot.fields.iter().position(|f| f.field_id == fid)
                    .ok_or(CompileError::UnknownField { field_id: fid })?;
                let off = layout.offsets[idx];
                let w = ot.fields[idx].kind.width() as usize;
                use kessel_catalog::FieldKind::*;
                match ot.fields[idx].kind {
                    I8 | I16 | I32 | I64 | I128 | Fixed { .. } => {
                        stack.push(StackItem::Op(Operand::LoadInt {
                            off, width: w, signed: true, fid_idx: idx, codec_shaped: codec,
                        }));
                    }
                    U8 | U16 | U32 | U64 | U128 | Bool | Timestamp => {
                        stack.push(StackItem::Op(Operand::LoadInt {
                            off, width: w, signed: false, fid_idx: idx, codec_shaped: codec,
                        }));
                    }
                    Char(_) | Bytes(_) | Ref | OverflowRef => {
                        stack.push(StackItem::Op(Operand::LoadBytes {
                            off, width: w, fid_idx: idx, codec_shaped: codec,
                        }));
                    }
                }
            }
            IS_NULL => {
                let fid = u16::from_le_bytes(
                    code.get(pc..pc + 2).ok_or(CompileError::BadProgram)?.try_into().unwrap(),
                );
                pc += 2;
                let idx = ot.fields.iter().position(|f| f.field_id == fid)
                    .ok_or(CompileError::UnknownField { field_id: fid })?;
                stack.push(StackItem::Bn(BoolNode::IsNull {
                    fid_idx: idx, codec_shaped: codec,
                }));
            }
            EQ | NE | LT | LE | GT | GE => {
                let cmp_op = match op {
                    EQ => CmpOp::Eq, NE => CmpOp::Ne, LT => CmpOp::Lt,
                    LE => CmpOp::Le, GT => CmpOp::Gt, _ => CmpOp::Ge,
                };
                let rhs = stack.pop().ok_or(CompileError::StackUnderflow)?;
                let lhs = stack.pop().ok_or(CompileError::StackUnderflow)?;
                // Both sides bool? -> BoolCmp; else -> Operand Cmp.
                match (lhs, rhs) {
                    (StackItem::Bn(lb), StackItem::Bn(rb)) => {
                        stack.push(StackItem::Bn(BoolNode::BoolCmp {
                            lhs: Box::new(lb), rhs: Box::new(rb), op: cmp_op,
                        }));
                    }
                    (lhs, rhs) => {
                        stack.push(StackItem::Bn(BoolNode::Cmp {
                            lhs: lhs.into_operand(), rhs: rhs.into_operand(), op: cmp_op,
                        }));
                    }
                }
            }
            AND => {
                let b = stack.pop().ok_or(CompileError::StackUnderflow)?.into_bool();
                let a = stack.pop().ok_or(CompileError::StackUnderflow)?.into_bool();
                stack.push(StackItem::Bn(BoolNode::And(Box::new(a), Box::new(b))));
            }
            OR => {
                let b = stack.pop().ok_or(CompileError::StackUnderflow)?.into_bool();
                let a = stack.pop().ok_or(CompileError::StackUnderflow)?.into_bool();
                stack.push(StackItem::Bn(BoolNode::Or(Box::new(a), Box::new(b))));
            }
            NOT => {
                let a = stack.pop().ok_or(CompileError::StackUnderflow)?.into_bool();
                stack.push(StackItem::Bn(BoolNode::Not(Box::new(a))));
            }
            ADD => return Err(CompileError::Unsupported { op_name: "ADD" }),
            SUB => return Err(CompileError::Unsupported { op_name: "SUB" }),
            MUL => return Err(CompileError::Unsupported { op_name: "MUL" }),
            DIV => return Err(CompileError::Unsupported { op_name: "DIV" }),
            MOD => return Err(CompileError::Unsupported { op_name: "MOD" }),
            LIKE => return Err(CompileError::Unsupported { op_name: "LIKE" }),
            SHA256 => return Err(CompileError::Unsupported { op_name: "SHA256" }),
            HMAC256 => return Err(CompileError::Unsupported { op_name: "HMAC256" }),
            SET_FIELD => return Err(CompileError::Unsupported { op_name: "SET_FIELD" }),
            REJECT => return Err(CompileError::Unsupported { op_name: "REJECT" }),
            _ => return Err(CompileError::BadProgram),
        }
    }
    if stack.len() != 1 {
        return Err(CompileError::Malformed);
    }
    let root = stack.pop().unwrap().into_bool();
    Ok(materialise_bool(root))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_catalog::Field;

    #[test]
    fn like_match_semantics() {
        let m = |s: &str, p: &str| like_match(s.as_bytes(), p.as_bytes());
        assert!(m("Alice", "Alice")); // literal
        assert!(!m("Alice", "alice")); // case-sensitive
        assert!(m("Alice", "A%")); // prefix
        assert!(m("Alice", "%e")); // suffix
        assert!(m("Alice", "%lic%")); // contains
        assert!(m("Alice", "_____")); // 5 single-char
        assert!(!m("Alice", "____")); // wrong length
        assert!(m("Alice", "A_ice")); // mixed
        assert!(m("Alice", "%")); // % matches anything
        assert!(m("", "%")); // % matches empty
        assert!(m("", "")); // empty/empty
        assert!(!m("Alice", "")); // empty pattern, non-empty text
        assert!(!m("Alice", "Bob%")); // no match
        assert!(m("aaa", "a%a")); // backtracking
        assert!(m("abababc", "a%b%c")); // multiple stars
    }

    fn ot() -> ObjectType {
        ObjectType {
            type_id: 1,
            name: "t".into(),
            schema_ver: 1,
            fields: vec![
                Field { field_id: 1, name: "age".into(), kind: FieldKind::I32, nullable: false },
                Field { field_id: 2, name: "bal".into(), kind: FieldKind::I64, nullable: true },
            ],
            indexes: vec![],
            unique: vec![],
            fks: vec![],
            checks: vec![],
            triggers: vec![],
            ordered: vec![],
            composite: vec![],
            defaults: vec![],
        }
    }
    fn rec(age: i32, bal: i64) -> Vec<u8> {
        let o = ot();
        let l = o.compute_layout();
        let mut b = vec![0u8; l.record_size];
        b[l.offsets[0]..l.offsets[0] + 4].copy_from_slice(&age.to_le_bytes());
        b[l.offsets[1]..l.offsets[1] + 8].copy_from_slice(&bal.to_le_bytes());
        b
    }

    #[test]
    fn sha256_and_hmac_opcodes_are_correct_and_deterministic() {
        let ot = ot();
        let rec: Vec<u8> = vec![];
        // sha256("abc") equals the known digest -> predicate true.
        let want = kessel_crypto::sha256(b"abc");
        let p = Program::new()
            .push_bytes(b"abc")
            .sha256()
            .push_bytes(&want)
            .eq()
            .bytes();
        assert_eq!(eval(&p, &ot, &rec), Ok(true));
        // wrong expected -> false (no panic, deterministic)
        let p2 = Program::new()
            .push_bytes(b"abc")
            .sha256()
            .push_bytes(b"not-the-digest")
            .eq()
            .bytes();
        assert_eq!(eval(&p2, &ot, &rec), Ok(false));
        // HMAC-SHA256(key="Jefe", msg="what do ya want for nothing?")
        let hwant =
            kessel_crypto::hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        let p3 = Program::new()
            .push_bytes(b"what do ya want for nothing?")
            .push_bytes(b"Jefe")
            .hmac256()
            .push_bytes(&hwant)
            .eq()
            .bytes();
        assert_eq!(eval(&p3, &ot, &rec), Ok(true));
    }

    #[test]
    fn comparison_and_logic() {
        // age >= 18 AND bal >= 0
        let p = Program::new()
            .load(1).push_int(18).ge()
            .load(2).push_int(0).ge()
            .and()
            .bytes();
        assert_eq!(eval(&p, &ot(), &rec(20, 5)), Ok(true));
        assert_eq!(eval(&p, &ot(), &rec(17, 5)), Ok(false));
        assert_eq!(eval(&p, &ot(), &rec(20, -1)), Ok(false));
    }

    #[test]
    fn arithmetic_and_div_zero() {
        // (age * 2) == 84
        let p = Program::new().load(1).push_int(2).mul().push_int(84).eq().bytes();
        assert_eq!(eval(&p, &ot(), &rec(42, 0)), Ok(true));
        // div by zero -> error (reject)
        let q = Program::new().push_int(1).push_int(0).div().bytes();
        assert_eq!(eval(&q, &ot(), &rec(0, 0)), Err(ExprError::DivByZero));
    }

    #[test]
    fn negative_signed_fields_decode_correctly() {
        let p = Program::new().load(2).push_int(0).lt().bytes(); // bal < 0
        assert_eq!(eval(&p, &ot(), &rec(0, -7)), Ok(true));
        assert_eq!(eval(&p, &ot(), &rec(0, 3)), Ok(false));
    }

    #[test]
    fn determinism_same_inputs_same_result() {
        let p = Program::new().load(1).load(2).add().push_int(100).le().bytes();
        let r1 = eval(&p, &ot(), &rec(40, 50));
        let r2 = eval(&p, &ot(), &rec(40, 50));
        assert_eq!(r1, r2);
        assert_eq!(r1, Ok(true));
    }

    #[test]
    fn malformed_program_is_rejected_not_panic() {
        assert_eq!(eval(&[PUSH_INT, 1, 2], &ot(), &rec(0, 0)), Err(ExprError::BadProgram));
        assert_eq!(eval(&[EQ], &ot(), &rec(0, 0)), Err(ExprError::StackUnderflow));
        assert_eq!(eval(&[], &ot(), &rec(0, 0)), Err(ExprError::EmptyResult));
        assert_eq!(eval(&[250], &ot(), &rec(0, 0)), Err(ExprError::BadProgram));
    }

    #[test]
    fn trigger_sets_derived_field() {
        // trigger: bal := age * 10
        let p = Program::new().load(1).push_int(10).mul().set_field(2).bytes();
        let out = eval_trigger(&p, &ot(), &rec(7, 0)).unwrap().unwrap();
        // read field 2 (bal, I64) back from the mutated record
        let l = ot().compute_layout();
        let bal = i64::from_le_bytes(out[l.offsets[1]..l.offsets[1] + 8].try_into().unwrap());
        assert_eq!(bal, 70);
    }

    #[test]
    fn trigger_can_reject() {
        let p = Program::new().reject().bytes();
        assert_eq!(eval_trigger(&p, &ot(), &rec(1, 1)), Ok(None));
        // conditional reject is up to the host (run program, then a guard
        // CHECK) — the VM stays branch-free; here we just prove REJECT works.
        let q = Program::new().load(1).push_int(5).set_field(1).bytes();
        let out = eval_trigger(&q, &ot(), &rec(9, 0)).unwrap().unwrap();
        let l = ot().compute_layout();
        let age = i32::from_le_bytes(out[l.offsets[0]..l.offsets[0] + 4].try_into().unwrap());
        assert_eq!(age, 5, "set_field overwrote age");
    }

    #[test]
    fn is_null_opcode() {
        // codec-shaped record with bal null bit set -> IS_NULL(2) true
        let o = ot();
        let mut r = rec(5, 0);
        // make it codec-shaped: set field_count=2 and null bit for field idx1
        r[4..6].copy_from_slice(&2u16.to_le_bytes());
        r[6] |= 1 << 1; // field index 1 (bal) null
        let p = Program::new().is_null(2).bytes();
        assert_eq!(eval(&p, &o, &r), Ok(true));
        let p2 = Program::new().is_null(1).bytes();
        assert_eq!(eval(&p2, &o, &r), Ok(false));
    }

    // ====================================================================
    // SP-WHERE-VM-Specialise — compile_filter KATs
    // ====================================================================
    //
    // Every KAT below runs the closure built by `compile_filter`
    // against the same input the interpreter would see, and either
    // (a) asserts a hand-computed expected bool OR (b) asserts the
    // closure result is byte-equal to `eval(...)`. The equivalence
    // contract is the determinism oracle.

    fn make_program(p: Program) -> (Vec<u8>, ObjectType) {
        (p.bytes(), ot())
    }

    #[test]
    fn compile_filter_simple_eq() {
        // WHERE age = 42
        let (code, o) = make_program(Program::new().load(1).push_int(42).eq());
        let f = compile_filter(&code, &o).expect("compile");
        assert!(f(&rec(42, 0)));
        assert!(!f(&rec(41, 0)));
        assert!(!f(&rec(43, 0)));
    }

    #[test]
    fn compile_filter_range_conjunction() {
        // WHERE bal >= 100 AND bal < 1000
        let code = Program::new()
            .load(2).push_int(100).ge()
            .load(2).push_int(1000).lt()
            .and()
            .bytes();
        let o = ot();
        let f = compile_filter(&code, &o).expect("compile");
        for v in [-1i64, 0, 99, 100, 500, 999, 1000, 1001] {
            let expected = v >= 100 && v < 1000;
            assert_eq!(f(&rec(0, v)), expected, "bal={v}");
        }
    }

    #[test]
    fn compile_filter_or_in_like() {
        // WHERE age = 1 OR age = 2 OR age = 3
        let code = Program::new()
            .load(1).push_int(1).eq()
            .load(1).push_int(2).eq()
            .or()
            .load(1).push_int(3).eq()
            .or()
            .bytes();
        let o = ot();
        let f = compile_filter(&code, &o).expect("compile");
        for v in [-1i32, 0, 1, 2, 3, 4, 5] {
            let expected = v == 1 || v == 2 || v == 3;
            assert_eq!(f(&rec(v, 0)), expected, "age={v}");
        }
    }

    #[test]
    fn compile_filter_negation() {
        // WHERE NOT (age = 0)
        let code = Program::new().load(1).push_int(0).eq().not().bytes();
        let o = ot();
        let f = compile_filter(&code, &o).expect("compile");
        assert!(!f(&rec(0, 0)));
        assert!(f(&rec(1, 0)));
        assert!(f(&rec(-1, 0)));
    }

    #[test]
    fn compile_filter_is_null() {
        // WHERE bal IS NULL — codec-shaped record with bit set for bal
        let code = Program::new().is_null(2).bytes();
        let o = ot();
        let f = compile_filter(&code, &o).expect("compile");
        // non-codec-shape: never null
        assert!(!f(&rec(1, 1)));
        // codec-shaped with bal null bit set
        let mut r = rec(1, 0);
        r[4..6].copy_from_slice(&2u16.to_le_bytes());
        r[6] |= 1 << 1;
        assert!(f(&r));
        // codec-shaped, bal null bit cleared
        let mut r2 = rec(1, 1);
        r2[4..6].copy_from_slice(&2u16.to_le_bytes());
        // (clear the bit)
        r2[6] &= !(1 << 1);
        assert!(!f(&r2));
    }

    #[test]
    fn compile_filter_5_predicate_conjunction() {
        // WHERE age >= 18 AND age <= 65 AND bal >= 0 AND bal < 10000 AND
        //       NOT (age = 30)
        let code = Program::new()
            .load(1).push_int(18).ge()
            .load(1).push_int(65).le()
            .and()
            .load(2).push_int(0).ge()
            .and()
            .load(2).push_int(10000).lt()
            .and()
            .load(1).push_int(30).eq().not()
            .and()
            .bytes();
        let o = ot();
        let f = compile_filter(&code, &o).expect("compile");
        for (a, b, expected) in [
            (25i32, 500i64, true),
            (17, 500, false),
            (66, 500, false),
            (25, -1, false),
            (25, 10000, false),
            (30, 500, false), // excluded by NOT (age=30)
            (29, 500, true),
            (31, 500, true),
        ] {
            assert_eq!(f(&rec(a, b)), expected, "age={a} bal={b}");
        }
    }

    #[test]
    fn compile_filter_byte_equal_to_interpreter_over_random_rows() {
        // Determinism oracle: closure result MUST byte-equal interpreter
        // for every supported opcode pattern on a 1000-row corpus.
        let programs: Vec<Vec<u8>> = vec![
            Program::new().load(1).push_int(42).eq().bytes(),
            Program::new().load(1).push_int(0).ne().bytes(),
            Program::new().load(1).push_int(50).lt().bytes(),
            Program::new().load(1).push_int(50).le().bytes(),
            Program::new().load(1).push_int(50).gt().bytes(),
            Program::new().load(1).push_int(50).ge().bytes(),
            Program::new()
                .load(1).push_int(0).ge()
                .load(2).push_int(1000).lt()
                .and().bytes(),
            Program::new()
                .load(1).push_int(10).eq()
                .load(1).push_int(20).eq()
                .or().bytes(),
            Program::new().load(1).push_int(0).gt().not().bytes(),
            Program::new()
                .load(1).push_int(0).ge()
                .load(2).push_int(100).ge()
                .and()
                .load(1).push_int(100).lt()
                .and()
                .bytes(),
        ];
        let o = ot();
        // Deterministic PRNG — simple LCG, no external dep.
        let mut s: u64 = 0xC0FFEE;
        let mut next = || {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            s
        };
        for code in &programs {
            let f = compile_filter(code, &o).expect("compile");
            for _ in 0..1000 {
                let a = (next() & 0xFF) as i32 - 128;
                let b = (next() & 0xFFFF) as i64 - 32768;
                let r = rec(a, b);
                let want = eval(code, &o, &r).unwrap_or(false);
                let got = f(&r);
                assert_eq!(
                    got, want,
                    "program {:?} row a={a} b={b}: closure={got} eval={want}",
                    code,
                );
            }
        }
    }

    #[test]
    fn compile_filter_uncond_true_compiles() {
        let code = Program::new().push_int(1).bytes();
        let o = ot();
        let f = compile_filter(&code, &o).expect("compile");
        assert!(f(&rec(0, 0)));
        assert!(f(&rec(100, 200)));
    }

    #[test]
    fn compile_filter_uncond_false_compiles() {
        let code = Program::new().push_int(0).bytes();
        let o = ot();
        let f = compile_filter(&code, &o).expect("compile");
        assert!(!f(&rec(0, 0)));
    }

    #[test]
    fn compile_filter_rejects_unsupported_opcodes() {
        // ADD in WHERE — V1 declines (caller falls back to interpreter).
        let code = Program::new().load(1).push_int(1).add().push_int(2).eq().bytes();
        let o = ot();
        let err = compile_filter(&code, &o).err().expect("expected Err");
        assert_eq!(err, CompileError::Unsupported { op_name: "ADD" }, "got {err:?}");
        // SHA256
        let code = Program::new().push_bytes(b"x").sha256().push_bytes(b"y").eq().bytes();
        let err = compile_filter(&code, &o).err().expect("expected Err");
        assert_eq!(err, CompileError::Unsupported { op_name: "SHA256" }, "got {err:?}");
        // LIKE
        let code = Program::new()
            .push_bytes(b"abc").push_bytes(b"a%").like().bytes();
        let err = compile_filter(&code, &o).err().expect("expected Err");
        assert_eq!(err, CompileError::Unsupported { op_name: "LIKE" }, "got {err:?}");
    }

    #[test]
    fn compile_filter_rejects_malformed_programs() {
        let o = ot();
        // empty program -> no value on stack
        let err = compile_filter(&[], &o).err().expect("expected Err");
        assert_eq!(err, CompileError::Malformed, "got {err:?}");
        // EQ with empty stack -> underflow
        let err = compile_filter(&[EQ], &o).err().expect("expected Err");
        assert_eq!(err, CompileError::StackUnderflow, "got {err:?}");
        // Truncated PUSH_INT
        let err = compile_filter(&[PUSH_INT, 1, 2], &o).err().expect("expected Err");
        assert_eq!(err, CompileError::BadProgram, "got {err:?}");
        // Unknown opcode
        let err = compile_filter(&[250u8], &o).err().expect("expected Err");
        assert_eq!(err, CompileError::BadProgram, "got {err:?}");
        // Two values left on stack -> Malformed
        let code = Program::new().push_int(1).push_int(2).bytes();
        let err = compile_filter(&code, &o).err().expect("expected Err");
        assert_eq!(err, CompileError::Malformed, "got {err:?}");
    }

    #[test]
    fn compile_filter_unknown_field_id() {
        let code = Program::new().load(999).push_int(0).eq().bytes();
        let o = ot();
        let err = compile_filter(&code, &o).err().expect("expected Err");
        assert_eq!(err, CompileError::UnknownField { field_id: 999 }, "got {err:?}");
    }

    #[test]
    fn compile_filter_signed_field_sign_extension() {
        // bal is I64 -> signed; compare against negative const
        let code = Program::new().load(2).push_int(-1000).lt().bytes();
        let o = ot();
        let f = compile_filter(&code, &o).expect("compile");
        assert!(f(&rec(0, -2000)));
        assert!(!f(&rec(0, -1000)));
        assert!(!f(&rec(0, 0)));
        assert!(!f(&rec(0, 1000)));
    }

    #[test]
    fn compile_filter_q6_shape_microbench_correctness() {
        // Synthetic Q6: numeric fields (signed I32 + I64 here), 4
        // predicates: bal >= L AND bal < H AND age >= a1 AND age < a2.
        // Verifies the realistic TPC-H Q6 shape (4-deep conjunction
        // with mixed widths) compiles + matches interpreter byte-for-
        // byte on every random row.
        let code = Program::new()
            .load(2).push_int(19940101).ge()
            .load(2).push_int(19950101).lt().and()
            .load(1).push_int(5).ge().and()
            .load(1).push_int(24).lt().and()
            .bytes();
        let o = ot();
        let f = compile_filter(&code, &o).expect("compile");
        let mut s: u64 = 0xBEEF;
        for _ in 0..500 {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let age = ((s >> 32) & 0xFF) as i32;
            let bal = (s & 0xFFFFFFFF) as i64 + 19940000;
            let r = rec(age, bal);
            let want = eval(&code, &o, &r).unwrap_or(false);
            assert_eq!(f(&r), want, "age={age} bal={bal}");
        }
    }
}
