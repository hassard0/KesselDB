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

/// Fluent program builder (test/host convenience). Emits the wire bytecode.
#[derive(Default)]
pub struct Program {
    code: Vec<u8>,
}

impl Program {
    pub fn new() -> Self {
        Self::default()
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
    if rec.len() != ot.compute_layout().record_size {
        return false;
    }
    let fc = u16::from_le_bytes(
        rec[SCHEMA_VER_BYTES..SCHEMA_VER_BYTES + 2].try_into().unwrap(),
    ) as usize;
    fc == ot.fields.len()
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

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_catalog::Field;

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
}
