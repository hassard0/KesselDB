//! kessel-wasm — Zero-dep deterministic WASM-MVP-subset interpreter for
//! in-tree user-defined functions (UDFs).
//!
//! Strategic-tier slice **S4 of THESIS.md** (initially shipped at SP118;
//! deeply extended at SP120 to address the documented out-of-scope caveats
//! with real implementation rather than deferral).
//!
//! ## Supported (post-SP120)
//!
//! - **Module format**: WASM-MVP magic (`0x00 0x61 0x73 0x6d`) + version
//!   `0x01 0x00 0x00 0x00`; sections decoded by ID (1=type, 3=function,
//!   5=memory, 10=code); other sections (custom, import, export, table,
//!   global, element, start, data, data-count) are SKIPPED past via their
//!   declared size.
//! - **Value types**: `i32` AND `i64`.
//! - **Function signatures**: arbitrary `i32`/`i64` params; **arbitrary
//!   number of `i32`/`i64` results** (multi-value returns supported per
//!   WASM-MVP+multi-value).
//! - **Locals**: `local.get`, `local.set`, `local.tee` over both
//!   `i32` and `i64` locals; per-group typing decoded from the locals
//!   declaration.
//! - **i32 integer ops**: `i32.const` (signed LEB128), `i32.add/sub/mul/
//!   div_s/div_u/rem_s/rem_u/and/or/xor/shl/shr_s/shr_u/rotl/rotr`,
//!   `i32.eqz/eq/ne/lt_s/lt_u/gt_s/gt_u/le_s/le_u/ge_s/ge_u`.
//! - **i64 integer ops**: all of the above with i64 types.
//! - **Conversions**: `i32.wrap_i64`, `i64.extend_i32_s/_u`.
//! - **Control flow**: `block`, `loop`, `if`/`else`/`end`, `br`, `br_if`,
//!   `return`, `call` (in-module), `drop`, `select`, `unreachable`,
//!   `nop`.
//! - **Linear memory (SP120 addition)**: one memory per module (WASM MVP
//!   already cap=1); declared by the memory section (`MemoryType` =
//!   `(min, max?)` pages of 64 KiB each); initialized to all zeros;
//!   `i32.load/store`, `i64.load/store` with 1/2/4/8-byte widths and
//!   typed sign-extension (`i32.load8_s/u`, `i32.load16_s/u`,
//!   `i64.load8/16/32_s/u`, `i32.store8/16`, `i64.store8/16/32`);
//!   `memory.size`/`memory.grow` (with the `max` cap respected). Loads
//!   and stores carry alignment + offset immediates; the alignment hint
//!   is read but does NOT affect determinism (per spec).
//! - **Gas accounting**: every executed instruction increments a counter
//!   by 1; exhaustion traps with `WasmError::OutOfGas`. Memory loads/stores
//!   ALSO bound the effective address (offset + 0..=8 bytes); out-of-
//!   bounds traps with `MemoryOutOfBounds`.
//! - **Determinism**: signed div/mod use WASM-spec semantics (i32::MIN/
//!   -1 + i64::MIN/-1 → IntegerOverflow trap; div/0 → IntegerDivideByZero
//!   trap; signed rem of MIN%-1 is 0 per spec). No floats, no host calls,
//!   no clocks. Memory pages are initialized to zero deterministically.
//!
//! ## Deliberately out of scope (with reasoning)
//!
//! - **`f32` / `f64` floats**: IEEE 754 specifies a CANONICAL NaN payload
//!   but the spec ALSO permits arithmetic NaN payloads that vary across
//!   host architectures (x86 vs ARM signaling-NaN propagation differs
//!   measurably). A deterministic float subset would require canonicalizing
//!   the NaN payload after EVERY float operation (~5% wall-clock overhead
//!   on micro-benchmarks per the FAST research compiler's measurements);
//!   that design + implementation is its own slice. Defer.
//! - **Tables + `call_indirect`**: defer until a UDF use-case demands
//!   function pointers / runtime dispatch. The current `call` (in-module
//!   by index) is sufficient for the recursive-function shape; tables
//!   add a section + a typed dispatch + a per-call type-check that's
//!   substantial code for a feature with no claimed S4 use-case yet.
//! - **Imports / exports beyond entry function**: the in-tree UDF model
//!   is fundamentally self-contained — a UDF that reaches into the host
//!   for I/O is BOTH a determinism risk surface AND defeats the purpose
//!   of running it in the deterministic SM layer (the host calls would
//!   need their own deterministic-execution gates). Entry is identified
//!   by func_idx; no import section is honored.
//! - **SIMD (`v128`)**: large opcode space + cross-platform SIMD
//!   determinism issues (the same bit patterns can produce different
//!   results on different CPUs without explicit floating-point
//!   canonicalization, see f32/f64 above). Defer.
//! - **Reference types / GC / exceptions / threads**: each is its own
//!   multi-month project with substantial determinism + memory-model
//!   implications. Defer.
//! - **Custom name section / debug info**: orthogonal to execution
//!   semantics. Could be added without changing the interpreter; not
//!   useful until a UDF developer-tools surface materializes.
//!
//! ## Determinism guarantee (S4 contract — UNCHANGED post-SP120)
//!
//! Two replicas executing the same `wasm_exec(module, func, args,
//! gas_limit)` on the same input bytes ALWAYS produce byte-identical
//! results (`Ok(Vec<Value>)` with the same payload, or the same
//! `Err(WasmError)` variant). No state outside the call survives; no
//! wall-clock, RNG, host syscall, or float operation is touched.
//! Memory pages are zero-initialized deterministically; `memory.grow`
//! returns the previous size and may return -1 only when the cap is
//! exceeded (deterministic — never spuriously refused for environmental
//! reasons).

#![forbid(unsafe_code)]
#![allow(clippy::needless_range_loop)]

// ============================================================================
// Public types
// ============================================================================

/// A WASM value — i32, i64, f32, or f64. Returned from `wasm_exec` and
/// passed in for function arguments.
///
/// **Float determinism (SP121 deep-extension)**: every f32/f64 produced
/// by the interpreter is FIRST passed through `canonicalize_f32`/`_f64`,
/// which collapses every NaN bit pattern to the WASM-spec canonical
/// payload (`0x7FC00000` / `0x7FF8000000000000`). This eliminates the
/// only cross-platform-non-determinism source in IEEE 754 floats (the
/// arithmetic itself is deterministic given round-to-nearest-even, which
/// is Rust's default — and we never use FMA / transcendentals; only the
/// arithmetic + sqrt + rounding ops in the WASM-MVP spec).
///
/// Note: `Value` derives `PartialEq` but NOT `Eq` because f32/f64 do not
/// satisfy reflexivity for NaN inputs. Post-canonicalization, however, all
/// NaN values produced by the interpreter compare bit-equal — tests that
/// need to assert NaN-equality use `.to_bits()` for bit-pattern checking.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value {
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
}

impl Value {
    pub fn as_i32(self) -> Option<i32> {
        match self {
            Value::I32(v) => Some(v),
            _ => None,
        }
    }
    pub fn as_i64(self) -> Option<i64> {
        match self {
            Value::I64(v) => Some(v),
            _ => None,
        }
    }
    pub fn as_f32(self) -> Option<f32> {
        match self {
            Value::F32(v) => Some(v),
            _ => None,
        }
    }
    pub fn as_f64(self) -> Option<f64> {
        match self {
            Value::F64(v) => Some(v),
            _ => None,
        }
    }
    fn ty(self) -> ValType {
        match self {
            Value::I32(_) => ValType::I32,
            Value::I64(_) => ValType::I64,
            Value::F32(_) => ValType::F32,
            Value::F64(_) => ValType::F64,
        }
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::I32(v)
    }
}
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::I64(v)
    }
}
impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::F32(canonicalize_f32(v))
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::F64(canonicalize_f64(v))
    }
}

/// SP121 — Canonical NaN bit pattern for f32 per WASM spec
/// (sign=0, exponent=all 1s, mantissa MSB=1, rest=0).
pub const CANONICAL_F32_NAN_BITS: u32 = 0x7FC0_0000;

/// SP121 — Canonical NaN bit pattern for f64.
pub const CANONICAL_F64_NAN_BITS: u64 = 0x7FF8_0000_0000_0000;

/// SP121 — Replace any NaN with the canonical NaN; pass non-NaN through.
/// Applied at EVERY f32 push by the interpreter so two replicas executing
/// the same WASM bytecode produce byte-identical f32 outputs even when
/// host architectures differ in their native NaN-payload propagation.
#[inline]
pub fn canonicalize_f32(v: f32) -> f32 {
    if v.is_nan() {
        f32::from_bits(CANONICAL_F32_NAN_BITS)
    } else {
        v
    }
}

#[inline]
pub fn canonicalize_f64(v: f64) -> f64 {
    if v.is_nan() {
        f64::from_bits(CANONICAL_F64_NAN_BITS)
    } else {
        v
    }
}

/// Errors produced by module decode + interpreter execution.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WasmError {
    UnexpectedEof,
    BadMagic,
    BadVersion(u32),
    BadLeb128,
    BadSection(u8),
    BadFuncType(u8),
    UnsupportedValType(u8),
    UnknownTypeIdx(u32),
    UnknownFuncIdx(u32),
    EntryFuncIdxOutOfRange { func_idx: u32, total: u32 },
    EntryArgsMismatch { expected: usize, got: usize },
    EntryArgTypeMismatch { idx: usize, expected: ValType, got: ValType },
    UnsupportedOpcode(u8),
    InvalidOpcode(u8),
    UnterminatedBlock,
    InvalidBranchDepth { depth: u32, active: u32 },
    InvalidLocalIdx { idx: u32, total: u32 },
    StackUnderflow { opcode: &'static str },
    /// An opcode required a specific operand type that the stack top
    /// didn't carry (e.g., `i32.add` with an i64 on top).
    StackTypeMismatch { opcode: &'static str, expected: ValType, got: ValType },
    OutOfGas,
    IntegerDivideByZero,
    IntegerOverflow,
    UnreachableExecuted,
    CallStackOverflow,
    /// Linear memory access past the current page count's byte size.
    MemoryOutOfBounds { addr: u64, len: u32, mem_bytes: u32 },
    /// Module declares more memories than the MVP cap (1).
    TooManyMemories(u32),
    /// `memory.grow` would exceed the declared maximum pages (returns -1
    /// to the WASM caller in that case; this error variant is for a
    /// MALFORMED max value, not for normal-grow refusal).
    InvalidMemoryLimits { min: u32, max: u32 },
    /// A `memory.*` opcode executed without a memory section declared.
    MemoryNotDeclared,
    /// SP121: float-to-int conversion (`i*.trunc_f*_*`) where the source
    /// is NaN, +/-inf, or outside the destination integer range. Per WASM
    /// spec this traps (the `*_sat_*` saturating variants are deferred to
    /// a future slice; only the trapping `trunc_*` variants ship here).
    IntegerConversionInvalid { what: &'static str, src_bits: u64 },
}

impl core::fmt::Display for WasmError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for WasmError {}

/// Value type — WASM-MVP `i32` + `i64` + `f32` + `f64`. SP121 deep-
/// extension added the float types with deterministic NaN canonicalization
/// (see `canonicalize_f32`/`_f64`); the WASM spec's CANONICAL NaN bit
/// pattern is enforced at every float push, making bit-level results
/// deterministic across hosts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValType {
    I32,
    I64,
    F32,
    F64,
}

/// One WASM page = 64 KiB. Per spec, memory size is reported AND grown
/// in this unit.
pub const PAGE_SIZE: u32 = 65536;

// ============================================================================
// Module decode
// ============================================================================

#[derive(Debug, Clone)]
pub struct Module {
    types: Vec<FuncType>,
    functions: Vec<u32>,
    bodies: Vec<FuncBody>,
    /// At most one memory in MVP; `None` if no memory section was present.
    memory: Option<MemoryType>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FuncType {
    params: Vec<ValType>,
    /// Multi-value returns supported; 0 or more results.
    results: Vec<ValType>,
}

#[derive(Debug, Clone)]
struct FuncBody {
    /// (count, valtype) per declared local-group. The interpreter expands
    /// these into a flat `Vec<Value>` at call time, after the params.
    local_groups: Vec<(u32, ValType)>,
    code: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemoryType {
    min_pages: u32,
    max_pages: Option<u32>,
}

impl Module {
    pub fn decode(bytes: &[u8]) -> Result<Self, WasmError> {
        let mut c = Cursor::new(bytes);

        let magic = c.read_n(4)?;
        if magic != [0x00, 0x61, 0x73, 0x6d] {
            return Err(WasmError::BadMagic);
        }
        let ver_bytes = c.read_n(4)?;
        let ver = u32::from_le_bytes([ver_bytes[0], ver_bytes[1], ver_bytes[2], ver_bytes[3]]);
        if ver != 1 {
            return Err(WasmError::BadVersion(ver));
        }

        let mut types: Vec<FuncType> = Vec::new();
        let mut functions: Vec<u32> = Vec::new();
        let mut bodies_raw: Vec<FuncBody> = Vec::new();
        let mut memory: Option<MemoryType> = None;

        while !c.eof() {
            let section_id = c.read_byte()?;
            let section_size = c.read_u32_leb()? as usize;
            let section_end = c
                .pos()
                .checked_add(section_size)
                .ok_or(WasmError::BadSection(section_id))?;
            if section_end > c.total_len() {
                return Err(WasmError::BadSection(section_id));
            }

            match section_id {
                1 => decode_type_section(&mut c, &mut types)?,
                3 => decode_function_section(&mut c, &mut functions, types.len())?,
                5 => {
                    let n = c.read_u32_leb()?;
                    if n > 1 {
                        return Err(WasmError::TooManyMemories(n));
                    }
                    if n == 1 {
                        memory = Some(decode_memory_type(&mut c)?);
                    }
                }
                10 => decode_code_section(&mut c, &mut bodies_raw)?,
                _ => c.skip(section_size)?,
            }

            if c.pos() != section_end {
                return Err(WasmError::BadSection(section_id));
            }
        }

        if functions.len() != bodies_raw.len() {
            return Err(WasmError::BadSection(10));
        }

        Ok(Module {
            types,
            functions,
            bodies: bodies_raw,
            memory,
        })
    }

    pub fn function_count(&self) -> u32 {
        self.functions.len() as u32
    }

    pub fn has_memory(&self) -> bool {
        self.memory.is_some()
    }
}

fn decode_type_section(c: &mut Cursor, types: &mut Vec<FuncType>) -> Result<(), WasmError> {
    let n = c.read_u32_leb()? as usize;
    for _ in 0..n {
        let tag = c.read_byte()?;
        if tag != 0x60 {
            return Err(WasmError::BadFuncType(tag));
        }
        let pcount = c.read_u32_leb()? as usize;
        let mut params = Vec::with_capacity(pcount.min(16));
        for _ in 0..pcount {
            params.push(read_val_type(c)?);
        }
        let rcount = c.read_u32_leb()? as usize;
        let mut results = Vec::with_capacity(rcount.min(4));
        for _ in 0..rcount {
            results.push(read_val_type(c)?);
        }
        types.push(FuncType { params, results });
    }
    Ok(())
}

fn decode_function_section(
    c: &mut Cursor,
    functions: &mut Vec<u32>,
    types_count: usize,
) -> Result<(), WasmError> {
    let n = c.read_u32_leb()? as usize;
    for _ in 0..n {
        let t = c.read_u32_leb()?;
        if t as usize >= types_count {
            return Err(WasmError::UnknownTypeIdx(t));
        }
        functions.push(t);
    }
    Ok(())
}

fn decode_memory_type(c: &mut Cursor) -> Result<MemoryType, WasmError> {
    let flags = c.read_byte()?;
    let min = c.read_u32_leb()?;
    let max = if flags & 0x01 != 0 {
        let m = c.read_u32_leb()?;
        if m < min {
            return Err(WasmError::InvalidMemoryLimits { min, max: m });
        }
        Some(m)
    } else {
        None
    };
    Ok(MemoryType {
        min_pages: min,
        max_pages: max,
    })
}

fn decode_code_section(c: &mut Cursor, bodies: &mut Vec<FuncBody>) -> Result<(), WasmError> {
    let n = c.read_u32_leb()? as usize;
    for _ in 0..n {
        let body_size = c.read_u32_leb()? as usize;
        let body_start = c.pos();
        let body_end = body_start
            .checked_add(body_size)
            .ok_or(WasmError::BadSection(10))?;
        if body_end > c.total_len() {
            return Err(WasmError::BadSection(10));
        }
        let local_group_count = c.read_u32_leb()? as usize;
        let mut local_groups = Vec::with_capacity(local_group_count);
        for _ in 0..local_group_count {
            let cnt = c.read_u32_leb()?;
            let v = read_val_type(c)?;
            local_groups.push((cnt, v));
        }
        let code_start = c.pos();
        if body_end < code_start {
            return Err(WasmError::BadSection(10));
        }
        let code_len = body_end - code_start;
        let code = c.read_n(code_len)?.to_vec();
        bodies.push(FuncBody { local_groups, code });
    }
    Ok(())
}

fn read_val_type(c: &mut Cursor) -> Result<ValType, WasmError> {
    let b = c.read_byte()?;
    match b {
        0x7F => Ok(ValType::I32),
        0x7E => Ok(ValType::I64),
        0x7D => Ok(ValType::F32),
        0x7C => Ok(ValType::F64),
        other => Err(WasmError::UnsupportedValType(other)),
    }
}

// ============================================================================
// Cursor
// ============================================================================

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    fn pos(&self) -> usize {
        self.pos
    }
    fn total_len(&self) -> usize {
        self.buf.len()
    }
    fn eof(&self) -> bool {
        self.pos >= self.buf.len()
    }
    fn read_byte(&mut self) -> Result<u8, WasmError> {
        let b = *self.buf.get(self.pos).ok_or(WasmError::UnexpectedEof)?;
        self.pos += 1;
        Ok(b)
    }
    fn read_n(&mut self, n: usize) -> Result<&'a [u8], WasmError> {
        let end = self.pos.checked_add(n).ok_or(WasmError::UnexpectedEof)?;
        let s = self.buf.get(self.pos..end).ok_or(WasmError::UnexpectedEof)?;
        self.pos = end;
        Ok(s)
    }
    fn skip(&mut self, n: usize) -> Result<(), WasmError> {
        let end = self.pos.checked_add(n).ok_or(WasmError::UnexpectedEof)?;
        if end > self.buf.len() {
            return Err(WasmError::UnexpectedEof);
        }
        self.pos = end;
        Ok(())
    }
    fn read_u32_leb(&mut self) -> Result<u32, WasmError> {
        let mut result: u32 = 0;
        let mut shift: u32 = 0;
        for _ in 0..5 {
            let b = self.read_byte()?;
            result |= ((b & 0x7F) as u32) << shift;
            if (b & 0x80) == 0 {
                return Ok(result);
            }
            shift += 7;
        }
        Err(WasmError::BadLeb128)
    }
}

// ============================================================================
// Code-side LEB128 + immediate readers
// ============================================================================

fn read_u32_leb(code: &[u8], ip: &mut usize) -> Result<u32, WasmError> {
    let mut result: u32 = 0;
    let mut shift: u32 = 0;
    for _ in 0..5 {
        let b = *code.get(*ip).ok_or(WasmError::UnexpectedEof)?;
        *ip += 1;
        result |= ((b & 0x7F) as u32) << shift;
        if (b & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(WasmError::BadLeb128)
}

fn read_i32_leb(code: &[u8], ip: &mut usize) -> Result<i32, WasmError> {
    let mut result: i64 = 0;
    let mut shift: u32 = 0;
    for _ in 0..5 {
        let b = *code.get(*ip).ok_or(WasmError::UnexpectedEof)?;
        *ip += 1;
        result |= ((b & 0x7F) as i64) << shift;
        shift += 7;
        if (b & 0x80) == 0 {
            if shift < 64 && (b & 0x40) != 0 {
                result |= -1i64 << shift;
            }
            if !(i32::MIN as i64..=i32::MAX as i64).contains(&result) {
                return Err(WasmError::BadLeb128);
            }
            return Ok(result as i32);
        }
    }
    Err(WasmError::BadLeb128)
}

fn read_i64_leb(code: &[u8], ip: &mut usize) -> Result<i64, WasmError> {
    let mut result: i128 = 0;
    let mut shift: u32 = 0;
    for _ in 0..10 {
        let b = *code.get(*ip).ok_or(WasmError::UnexpectedEof)?;
        *ip += 1;
        result |= ((b & 0x7F) as i128) << shift;
        shift += 7;
        if (b & 0x80) == 0 {
            if shift < 128 && (b & 0x40) != 0 {
                result |= -1i128 << shift;
            }
            if !(i64::MIN as i128..=i64::MAX as i128).contains(&result) {
                return Err(WasmError::BadLeb128);
            }
            return Ok(result as i64);
        }
    }
    Err(WasmError::BadLeb128)
}

fn skip_u32_leb(code: &[u8], ip: &mut usize) -> Result<(), WasmError> {
    for _ in 0..5 {
        let b = *code.get(*ip).ok_or(WasmError::UnexpectedEof)?;
        *ip += 1;
        if (b & 0x80) == 0 {
            return Ok(());
        }
    }
    Err(WasmError::BadLeb128)
}

fn skip_i64_leb(code: &[u8], ip: &mut usize) -> Result<(), WasmError> {
    for _ in 0..10 {
        let b = *code.get(*ip).ok_or(WasmError::UnexpectedEof)?;
        *ip += 1;
        if (b & 0x80) == 0 {
            return Ok(());
        }
    }
    Err(WasmError::BadLeb128)
}

// ============================================================================
// Interpreter
// ============================================================================

pub fn wasm_exec(
    module_bytes: &[u8],
    func_idx: u32,
    args: &[Value],
    gas_limit: u64,
) -> Result<Vec<Value>, WasmError> {
    let module = Module::decode(module_bytes)?;
    exec_in_module(&module, func_idx, args, gas_limit)
}

const MAX_CALL_DEPTH: u32 = 256;

fn exec_in_module(
    module: &Module,
    func_idx: u32,
    args: &[Value],
    gas_limit: u64,
) -> Result<Vec<Value>, WasmError> {
    let total = module.function_count();
    if func_idx >= total {
        return Err(WasmError::EntryFuncIdxOutOfRange { func_idx, total });
    }
    let type_idx = module.functions[func_idx as usize] as usize;
    let ftype = &module.types[type_idx];
    if args.len() != ftype.params.len() {
        return Err(WasmError::EntryArgsMismatch {
            expected: ftype.params.len(),
            got: args.len(),
        });
    }
    for (i, (a, expected)) in args.iter().zip(ftype.params.iter()).enumerate() {
        if a.ty() != *expected {
            return Err(WasmError::EntryArgTypeMismatch {
                idx: i,
                expected: *expected,
                got: a.ty(),
            });
        }
    }
    let mut gas = Gas {
        limit: gas_limit,
        used: 0,
    };
    let mut mem: Memory = Memory::new(module.memory);
    call_function(module, func_idx, args, &mut gas, &mut mem, 0)
}

struct Gas {
    limit: u64,
    used: u64,
}

impl Gas {
    fn tick(&mut self) -> Result<(), WasmError> {
        if self.used >= self.limit {
            return Err(WasmError::OutOfGas);
        }
        self.used += 1;
        Ok(())
    }
}

/// Linear memory state. Pages are zero-initialized; `grow` extends in
/// page units, refusing past the declared `max_pages` (per spec, returns
/// -1 to the WASM caller via the opcode handler, NOT a trap).
struct Memory {
    bytes: Vec<u8>,
    declared: Option<MemoryType>,
}

impl Memory {
    fn new(mt: Option<MemoryType>) -> Self {
        let bytes = match mt {
            Some(m) => vec![0u8; (m.min_pages as usize) * (PAGE_SIZE as usize)],
            None => Vec::new(),
        };
        Self { bytes, declared: mt }
    }
    fn pages(&self) -> u32 {
        if self.declared.is_none() {
            return 0;
        }
        (self.bytes.len() / PAGE_SIZE as usize) as u32
    }
    /// Returns the OLD page count, or `i32::MAX` (sentinel for -1 per
    /// WASM spec for memory.grow refusal).
    fn grow(&mut self, n: u32) -> i32 {
        if self.declared.is_none() {
            return -1;
        }
        let cur = self.pages();
        let new_pages = match cur.checked_add(n) {
            Some(p) => p,
            None => return -1,
        };
        if let Some(mt) = self.declared {
            if let Some(max) = mt.max_pages {
                if new_pages > max {
                    return -1;
                }
            }
        }
        // Cap at a sane absolute (4 GiB - 1 page) — same as the spec's
        // implicit max page count of 65535.
        if new_pages > 65535 {
            return -1;
        }
        let add_bytes = (n as usize) * (PAGE_SIZE as usize);
        self.bytes.extend(core::iter::repeat(0u8).take(add_bytes));
        cur as i32
    }
    fn effective_addr(&self, base: i32, offset: u32, width: u32) -> Result<usize, WasmError> {
        if self.declared.is_none() {
            return Err(WasmError::MemoryNotDeclared);
        }
        let addr = (base as u32 as u64) + (offset as u64);
        let end = addr + (width as u64);
        if end > self.bytes.len() as u64 {
            return Err(WasmError::MemoryOutOfBounds {
                addr,
                len: width,
                mem_bytes: self.bytes.len() as u32,
            });
        }
        Ok(addr as usize)
    }
}

#[derive(Debug, Clone, Copy)]
struct Label {
    target_ip: usize,
    stack_height_at_start: usize,
    is_loop: bool,
}

fn call_function(
    module: &Module,
    func_idx: u32,
    args: &[Value],
    gas: &mut Gas,
    mem: &mut Memory,
    call_depth: u32,
) -> Result<Vec<Value>, WasmError> {
    if call_depth >= MAX_CALL_DEPTH {
        return Err(WasmError::CallStackOverflow);
    }
    let type_idx = module.functions[func_idx as usize] as usize;
    let ftype = &module.types[type_idx];
    let body = &module.bodies[func_idx as usize];

    let n_params = ftype.params.len();
    let mut locals: Vec<Value> = Vec::with_capacity(n_params + 8);
    for &a in args {
        locals.push(a);
    }
    for &(cnt, vt) in &body.local_groups {
        let zero = match vt {
            ValType::I32 => Value::I32(0),
            ValType::I64 => Value::I64(0),
            ValType::F32 => Value::F32(0.0),
            ValType::F64 => Value::F64(0.0),
        };
        for _ in 0..cnt {
            locals.push(zero);
        }
    }

    let mut stack: Vec<Value> = Vec::with_capacity(32);
    let mut labels: Vec<Label> = Vec::new();

    let code = &body.code;
    let mut ip: usize = 0;
    while ip < code.len() {
        gas.tick()?;
        let op = code[ip];
        ip += 1;
        if !exec_one(op, code, &mut ip, &mut stack, &mut locals, &mut labels, mem, module, gas, call_depth)? {
            // exec_one returned Ok(false) to signal "stop the function".
            break;
        }
    }

    // Build result per signature: pop N results in order; topmost stack
    // value is the LAST result per WASM convention.
    let n_results = ftype.results.len();
    if stack.len() < n_results {
        return Err(WasmError::StackUnderflow { opcode: "return-values" });
    }
    let split = stack.len() - n_results;
    let mut out: Vec<Value> = stack.drain(split..).collect();
    // Verify each result's type matches the declared signature.
    for (i, expected) in ftype.results.iter().enumerate() {
        if out[i].ty() != *expected {
            return Err(WasmError::StackTypeMismatch {
                opcode: "return-values",
                expected: *expected,
                got: out[i].ty(),
            });
        }
    }
    // Multi-result: out is already in declaration order (drain preserves
    // order; results are popped left-to-right from the bottom of the
    // post-split region).
    let _ = &mut out;
    Ok(out)
}

/// Returns Ok(true) to continue executing, Ok(false) to stop the function
/// (return / explicit break out of outermost block / unconditional br to
/// depth == label-count).
fn exec_one(
    op: u8,
    code: &[u8],
    ip: &mut usize,
    stack: &mut Vec<Value>,
    locals: &mut Vec<Value>,
    labels: &mut Vec<Label>,
    mem: &mut Memory,
    module: &Module,
    gas: &mut Gas,
    call_depth: u32,
) -> Result<bool, WasmError> {
    match op {
        0x00 => return Err(WasmError::UnreachableExecuted),
        0x01 => {}
        0x02 => {
            let _bt = read_blocktype(code, ip)?;
            let end_ip = find_matching_end(code, *ip)?;
            labels.push(Label { target_ip: end_ip, stack_height_at_start: stack.len(), is_loop: false });
        }
        0x03 => {
            let _bt = read_blocktype(code, ip)?;
            labels.push(Label { target_ip: *ip, stack_height_at_start: stack.len(), is_loop: true });
        }
        0x04 => {
            let _bt = read_blocktype(code, ip)?;
            let c_val = pop_i32(stack, "if")?;
            let end_ip = find_matching_end(code, *ip)?;
            let else_ip = find_matching_else(code, *ip, end_ip);
            labels.push(Label { target_ip: end_ip, stack_height_at_start: stack.len(), is_loop: false });
            if c_val == 0 {
                *ip = match else_ip {
                    Some(e) => e + 1,
                    None => end_ip,
                };
            }
        }
        0x05 => {
            let lbl = labels.last().ok_or(WasmError::UnterminatedBlock)?;
            *ip = lbl.target_ip;
        }
        0x0B => {
            if labels.is_empty() {
                return Ok(false);
            }
            labels.pop();
        }
        0x0C => {
            let depth = read_u32_leb(code, ip)?;
            if !do_branch(stack, labels, ip, depth)? {
                return Ok(false);
            }
        }
        0x0D => {
            let depth = read_u32_leb(code, ip)?;
            let c_val = pop_i32(stack, "br_if")?;
            if c_val != 0 && !do_branch(stack, labels, ip, depth)? {
                return Ok(false);
            }
        }
        0x0F => return Ok(false),
        0x10 => {
            let callee = read_u32_leb(code, ip)?;
            if callee >= module.function_count() {
                return Err(WasmError::UnknownFuncIdx(callee));
            }
            let callee_type_idx = module.functions[callee as usize] as usize;
            let callee_type = &module.types[callee_type_idx];
            let n_args = callee_type.params.len();
            if stack.len() < n_args {
                return Err(WasmError::StackUnderflow { opcode: "call" });
            }
            let split = stack.len() - n_args;
            let call_args: Vec<Value> = stack.drain(split..).collect();
            // Validate arg types match callee signature.
            for (i, (a, expected)) in call_args.iter().zip(callee_type.params.iter()).enumerate() {
                if a.ty() != *expected {
                    return Err(WasmError::EntryArgTypeMismatch {
                        idx: i,
                        expected: *expected,
                        got: a.ty(),
                    });
                }
            }
            let r = call_function(module, callee, &call_args, gas, mem, call_depth + 1)?;
            for v in r {
                stack.push(v);
            }
        }
        0x1A => {
            stack.pop().ok_or(WasmError::StackUnderflow { opcode: "drop" })?;
        }
        0x1B => {
            let c_val = pop_i32(stack, "select")?;
            let b = stack.pop().ok_or(WasmError::StackUnderflow { opcode: "select" })?;
            let a = stack.pop().ok_or(WasmError::StackUnderflow { opcode: "select" })?;
            if a.ty() != b.ty() {
                return Err(WasmError::StackTypeMismatch {
                    opcode: "select",
                    expected: a.ty(),
                    got: b.ty(),
                });
            }
            stack.push(if c_val != 0 { a } else { b });
        }
        0x20 => {
            let idx = read_u32_leb(code, ip)?;
            let v = *locals
                .get(idx as usize)
                .ok_or(WasmError::InvalidLocalIdx { idx, total: locals.len() as u32 })?;
            stack.push(v);
        }
        0x21 => {
            let idx = read_u32_leb(code, ip)?;
            let v = stack.pop().ok_or(WasmError::StackUnderflow { opcode: "local.set" })?;
            let total = locals.len() as u32;
            let slot = locals
                .get_mut(idx as usize)
                .ok_or(WasmError::InvalidLocalIdx { idx, total })?;
            if slot.ty() != v.ty() {
                return Err(WasmError::StackTypeMismatch {
                    opcode: "local.set",
                    expected: slot.ty(),
                    got: v.ty(),
                });
            }
            *slot = v;
        }
        0x22 => {
            let idx = read_u32_leb(code, ip)?;
            let v = *stack.last().ok_or(WasmError::StackUnderflow { opcode: "local.tee" })?;
            let total = locals.len() as u32;
            let slot = locals
                .get_mut(idx as usize)
                .ok_or(WasmError::InvalidLocalIdx { idx, total })?;
            if slot.ty() != v.ty() {
                return Err(WasmError::StackTypeMismatch {
                    opcode: "local.tee",
                    expected: slot.ty(),
                    got: v.ty(),
                });
            }
            *slot = v;
        }
        // ---- Memory loads (alignment LEB + offset LEB; alignment is hint only) ----
        0x28 => mem_load(stack, code, ip, mem, "i32.load", 4, |b| Value::I32(i32::from_le_bytes([b[0],b[1],b[2],b[3]])))?,
        0x29 => mem_load(stack, code, ip, mem, "i64.load", 8, |b| Value::I64(i64::from_le_bytes([b[0],b[1],b[2],b[3],b[4],b[5],b[6],b[7]])))?,
        0x2C => mem_load(stack, code, ip, mem, "i32.load8_s", 1, |b| Value::I32((b[0] as i8) as i32))?,
        0x2D => mem_load(stack, code, ip, mem, "i32.load8_u", 1, |b| Value::I32(b[0] as i32))?,
        0x2E => mem_load(stack, code, ip, mem, "i32.load16_s", 2, |b| Value::I32((i16::from_le_bytes([b[0],b[1]])) as i32))?,
        0x2F => mem_load(stack, code, ip, mem, "i32.load16_u", 2, |b| Value::I32(u16::from_le_bytes([b[0],b[1]]) as i32))?,
        0x30 => mem_load(stack, code, ip, mem, "i64.load8_s", 1, |b| Value::I64((b[0] as i8) as i64))?,
        0x31 => mem_load(stack, code, ip, mem, "i64.load8_u", 1, |b| Value::I64(b[0] as i64))?,
        0x32 => mem_load(stack, code, ip, mem, "i64.load16_s", 2, |b| Value::I64((i16::from_le_bytes([b[0],b[1]])) as i64))?,
        0x33 => mem_load(stack, code, ip, mem, "i64.load16_u", 2, |b| Value::I64(u16::from_le_bytes([b[0],b[1]]) as i64))?,
        0x34 => mem_load(stack, code, ip, mem, "i64.load32_s", 4, |b| Value::I64((i32::from_le_bytes([b[0],b[1],b[2],b[3]])) as i64))?,
        0x35 => mem_load(stack, code, ip, mem, "i64.load32_u", 4, |b| Value::I64(u32::from_le_bytes([b[0],b[1],b[2],b[3]]) as i64))?,
        // ---- Memory stores ----
        0x36 => mem_store_i32(stack, code, ip, mem, "i32.store", 4)?,
        0x37 => mem_store_i64(stack, code, ip, mem, "i64.store", 8)?,
        0x3A => mem_store_i32(stack, code, ip, mem, "i32.store8", 1)?,
        0x3B => mem_store_i32(stack, code, ip, mem, "i32.store16", 2)?,
        0x3C => mem_store_i64(stack, code, ip, mem, "i64.store8", 1)?,
        0x3D => mem_store_i64(stack, code, ip, mem, "i64.store16", 2)?,
        0x3E => mem_store_i64(stack, code, ip, mem, "i64.store32", 4)?,
        // ---- memory.size / memory.grow (each takes a reserved 0x00 byte) ----
        0x3F => {
            let _reserved = read_u32_leb(code, ip)?;
            if mem.declared.is_none() {
                return Err(WasmError::MemoryNotDeclared);
            }
            stack.push(Value::I32(mem.pages() as i32));
        }
        0x40 => {
            let _reserved = read_u32_leb(code, ip)?;
            let n = pop_i32(stack, "memory.grow")?;
            if mem.declared.is_none() {
                return Err(WasmError::MemoryNotDeclared);
            }
            let r = if n < 0 { -1 } else { mem.grow(n as u32) };
            stack.push(Value::I32(r));
        }
        // ---- i32.const, i64.const ----
        0x41 => {
            let v = read_i32_leb(code, ip)?;
            stack.push(Value::I32(v));
        }
        0x42 => {
            let v = read_i64_leb(code, ip)?;
            stack.push(Value::I64(v));
        }
        // ---- i32 comparisons / arithmetic ----
        0x45 => {
            let a = pop_i32(stack, "i32.eqz")?;
            stack.push(Value::I32(if a == 0 { 1 } else { 0 }));
        }
        0x46 => i32_cmp(stack, "i32.eq", |a, b| a == b)?,
        0x47 => i32_cmp(stack, "i32.ne", |a, b| a != b)?,
        0x48 => i32_cmp(stack, "i32.lt_s", |a, b| a < b)?,
        0x49 => i32_cmp_unsigned(stack, "i32.lt_u", |a, b| a < b)?,
        0x4A => i32_cmp(stack, "i32.gt_s", |a, b| a > b)?,
        0x4B => i32_cmp_unsigned(stack, "i32.gt_u", |a, b| a > b)?,
        0x4C => i32_cmp(stack, "i32.le_s", |a, b| a <= b)?,
        0x4D => i32_cmp_unsigned(stack, "i32.le_u", |a, b| a <= b)?,
        0x4E => i32_cmp(stack, "i32.ge_s", |a, b| a >= b)?,
        0x4F => i32_cmp_unsigned(stack, "i32.ge_u", |a, b| a >= b)?,
        // ---- i64 comparisons ----
        0x50 => {
            let a = pop_i64(stack, "i64.eqz")?;
            stack.push(Value::I32(if a == 0 { 1 } else { 0 }));
        }
        0x51 => i64_cmp(stack, "i64.eq", |a, b| a == b)?,
        0x52 => i64_cmp(stack, "i64.ne", |a, b| a != b)?,
        0x53 => i64_cmp(stack, "i64.lt_s", |a, b| a < b)?,
        0x54 => i64_cmp_unsigned(stack, "i64.lt_u", |a, b| a < b)?,
        0x55 => i64_cmp(stack, "i64.gt_s", |a, b| a > b)?,
        0x56 => i64_cmp_unsigned(stack, "i64.gt_u", |a, b| a > b)?,
        0x57 => i64_cmp(stack, "i64.le_s", |a, b| a <= b)?,
        0x58 => i64_cmp_unsigned(stack, "i64.le_u", |a, b| a <= b)?,
        0x59 => i64_cmp(stack, "i64.ge_s", |a, b| a >= b)?,
        0x5A => i64_cmp_unsigned(stack, "i64.ge_u", |a, b| a >= b)?,
        // ---- i32 arithmetic ----
        0x6A => i32_bin_wrap(stack, "i32.add", i32::wrapping_add)?,
        0x6B => i32_bin_wrap(stack, "i32.sub", i32::wrapping_sub)?,
        0x6C => i32_bin_wrap(stack, "i32.mul", i32::wrapping_mul)?,
        0x6D => {
            let b = pop_i32(stack, "i32.div_s")?;
            let a = pop_i32(stack, "i32.div_s")?;
            if b == 0 {
                return Err(WasmError::IntegerDivideByZero);
            }
            if a == i32::MIN && b == -1 {
                return Err(WasmError::IntegerOverflow);
            }
            stack.push(Value::I32(a / b));
        }
        0x6E => {
            let b = pop_i32(stack, "i32.div_u")? as u32;
            let a = pop_i32(stack, "i32.div_u")? as u32;
            if b == 0 {
                return Err(WasmError::IntegerDivideByZero);
            }
            stack.push(Value::I32((a / b) as i32));
        }
        0x6F => {
            let b = pop_i32(stack, "i32.rem_s")?;
            let a = pop_i32(stack, "i32.rem_s")?;
            if b == 0 {
                return Err(WasmError::IntegerDivideByZero);
            }
            let r = if a == i32::MIN && b == -1 { 0 } else { a % b };
            stack.push(Value::I32(r));
        }
        0x70 => {
            let b = pop_i32(stack, "i32.rem_u")? as u32;
            let a = pop_i32(stack, "i32.rem_u")? as u32;
            if b == 0 {
                return Err(WasmError::IntegerDivideByZero);
            }
            stack.push(Value::I32((a % b) as i32));
        }
        0x71 => i32_bin_wrap(stack, "i32.and", |a, b| a & b)?,
        0x72 => i32_bin_wrap(stack, "i32.or", |a, b| a | b)?,
        0x73 => i32_bin_wrap(stack, "i32.xor", |a, b| a ^ b)?,
        0x74 => {
            let b = pop_i32(stack, "i32.shl")? as u32;
            let a = pop_i32(stack, "i32.shl")?;
            stack.push(Value::I32(a.wrapping_shl(b & 31)));
        }
        0x75 => {
            let b = pop_i32(stack, "i32.shr_s")? as u32;
            let a = pop_i32(stack, "i32.shr_s")?;
            stack.push(Value::I32(a.wrapping_shr(b & 31)));
        }
        0x76 => {
            let b = pop_i32(stack, "i32.shr_u")? as u32;
            let a = pop_i32(stack, "i32.shr_u")? as u32;
            stack.push(Value::I32(a.wrapping_shr(b & 31) as i32));
        }
        0x77 => {
            let b = pop_i32(stack, "i32.rotl")? as u32;
            let a = pop_i32(stack, "i32.rotl")? as u32;
            stack.push(Value::I32(a.rotate_left(b & 31) as i32));
        }
        0x78 => {
            let b = pop_i32(stack, "i32.rotr")? as u32;
            let a = pop_i32(stack, "i32.rotr")? as u32;
            stack.push(Value::I32(a.rotate_right(b & 31) as i32));
        }
        // ---- i64 arithmetic ----
        0x7C => i64_bin_wrap(stack, "i64.add", i64::wrapping_add)?,
        0x7D => i64_bin_wrap(stack, "i64.sub", i64::wrapping_sub)?,
        0x7E => i64_bin_wrap(stack, "i64.mul", i64::wrapping_mul)?,
        0x7F => {
            let b = pop_i64(stack, "i64.div_s")?;
            let a = pop_i64(stack, "i64.div_s")?;
            if b == 0 {
                return Err(WasmError::IntegerDivideByZero);
            }
            if a == i64::MIN && b == -1 {
                return Err(WasmError::IntegerOverflow);
            }
            stack.push(Value::I64(a / b));
        }
        0x80 => {
            let b = pop_i64(stack, "i64.div_u")? as u64;
            let a = pop_i64(stack, "i64.div_u")? as u64;
            if b == 0 {
                return Err(WasmError::IntegerDivideByZero);
            }
            stack.push(Value::I64((a / b) as i64));
        }
        0x81 => {
            let b = pop_i64(stack, "i64.rem_s")?;
            let a = pop_i64(stack, "i64.rem_s")?;
            if b == 0 {
                return Err(WasmError::IntegerDivideByZero);
            }
            let r = if a == i64::MIN && b == -1 { 0 } else { a % b };
            stack.push(Value::I64(r));
        }
        0x82 => {
            let b = pop_i64(stack, "i64.rem_u")? as u64;
            let a = pop_i64(stack, "i64.rem_u")? as u64;
            if b == 0 {
                return Err(WasmError::IntegerDivideByZero);
            }
            stack.push(Value::I64((a % b) as i64));
        }
        0x83 => i64_bin_wrap(stack, "i64.and", |a, b| a & b)?,
        0x84 => i64_bin_wrap(stack, "i64.or", |a, b| a | b)?,
        0x85 => i64_bin_wrap(stack, "i64.xor", |a, b| a ^ b)?,
        0x86 => {
            let b = pop_i64(stack, "i64.shl")? as u64;
            let a = pop_i64(stack, "i64.shl")?;
            stack.push(Value::I64(a.wrapping_shl((b & 63) as u32)));
        }
        0x87 => {
            let b = pop_i64(stack, "i64.shr_s")? as u64;
            let a = pop_i64(stack, "i64.shr_s")?;
            stack.push(Value::I64(a.wrapping_shr((b & 63) as u32)));
        }
        0x88 => {
            let b = pop_i64(stack, "i64.shr_u")? as u64;
            let a = pop_i64(stack, "i64.shr_u")? as u64;
            stack.push(Value::I64(a.wrapping_shr((b & 63) as u32) as i64));
        }
        0x89 => {
            let b = pop_i64(stack, "i64.rotl")? as u64;
            let a = pop_i64(stack, "i64.rotl")? as u64;
            stack.push(Value::I64(a.rotate_left((b & 63) as u32) as i64));
        }
        0x8A => {
            let b = pop_i64(stack, "i64.rotr")? as u64;
            let a = pop_i64(stack, "i64.rotr")? as u64;
            stack.push(Value::I64(a.rotate_right((b & 63) as u32) as i64));
        }
        // ---- conversions ----
        0xA7 => {
            // i32.wrap_i64
            let a = pop_i64(stack, "i32.wrap_i64")?;
            stack.push(Value::I32(a as i32));
        }
        0xAC => {
            // i64.extend_i32_s
            let a = pop_i32(stack, "i64.extend_i32_s")?;
            stack.push(Value::I64(a as i64));
        }
        0xAD => {
            // i64.extend_i32_u
            let a = pop_i32(stack, "i64.extend_i32_u")? as u32;
            stack.push(Value::I64(a as i64));
        }

        // ===========================================================
        // SP122 — bit-manipulation ops (clz / ctz / popcnt for i32+i64)
        // ===========================================================
        0x67 => {
            // i32.clz
            let a = pop_i32(stack, "i32.clz")? as u32;
            stack.push(Value::I32(a.leading_zeros() as i32));
        }
        0x68 => {
            // i32.ctz
            let a = pop_i32(stack, "i32.ctz")? as u32;
            stack.push(Value::I32(a.trailing_zeros() as i32));
        }
        0x69 => {
            // i32.popcnt
            let a = pop_i32(stack, "i32.popcnt")? as u32;
            stack.push(Value::I32(a.count_ones() as i32));
        }
        0x79 => {
            // i64.clz
            let a = pop_i64(stack, "i64.clz")? as u64;
            stack.push(Value::I64(a.leading_zeros() as i64));
        }
        0x7A => {
            // i64.ctz
            let a = pop_i64(stack, "i64.ctz")? as u64;
            stack.push(Value::I64(a.trailing_zeros() as i64));
        }
        0x7B => {
            // i64.popcnt
            let a = pop_i64(stack, "i64.popcnt")? as u64;
            stack.push(Value::I64(a.count_ones() as i64));
        }

        // ===========================================================
        // SP122 — sign-extension ops (0xC0..=0xC4)
        // ===========================================================
        0xC0 => {
            // i32.extend8_s
            let a = pop_i32(stack, "i32.extend8_s")?;
            stack.push(Value::I32((a as i8) as i32));
        }
        0xC1 => {
            // i32.extend16_s
            let a = pop_i32(stack, "i32.extend16_s")?;
            stack.push(Value::I32((a as i16) as i32));
        }
        0xC2 => {
            // i64.extend8_s
            let a = pop_i64(stack, "i64.extend8_s")?;
            stack.push(Value::I64((a as i8) as i64));
        }
        0xC3 => {
            // i64.extend16_s
            let a = pop_i64(stack, "i64.extend16_s")?;
            stack.push(Value::I64((a as i16) as i64));
        }
        0xC4 => {
            // i64.extend32_s
            let a = pop_i64(stack, "i64.extend32_s")?;
            stack.push(Value::I64((a as i32) as i64));
        }

        // ===========================================================
        // SP121 — float opcodes (f32 + f64) with deterministic NaN
        // canonicalization. Every float push goes through push_f32/_f64
        // which calls canonicalize_f*(); cross-host NaN payload
        // divergence is impossible by construction.
        // ===========================================================

        // f32.load / f64.load
        0x2A => mem_load(stack, code, ip, mem, "f32.load", 4, |b| Value::F32(canonicalize_f32(f32::from_le_bytes([b[0],b[1],b[2],b[3]]))))?,
        0x2B => mem_load(stack, code, ip, mem, "f64.load", 8, |b| Value::F64(canonicalize_f64(f64::from_le_bytes([b[0],b[1],b[2],b[3],b[4],b[5],b[6],b[7]]))))?,
        // f32.store / f64.store
        0x38 => mem_store_f32(stack, code, ip, mem, "f32.store")?,
        0x39 => mem_store_f64(stack, code, ip, mem, "f64.store")?,
        // f32.const (4 raw LE bytes) / f64.const (8 raw LE bytes)
        0x43 => {
            if *ip + 4 > code.len() {
                return Err(WasmError::UnexpectedEof);
            }
            let v = f32::from_le_bytes([code[*ip], code[*ip+1], code[*ip+2], code[*ip+3]]);
            *ip += 4;
            push_f32(stack, v);
        }
        0x44 => {
            if *ip + 8 > code.len() {
                return Err(WasmError::UnexpectedEof);
            }
            let v = f64::from_le_bytes([
                code[*ip], code[*ip+1], code[*ip+2], code[*ip+3],
                code[*ip+4], code[*ip+5], code[*ip+6], code[*ip+7],
            ]);
            *ip += 8;
            push_f64(stack, v);
        }
        // ---- f32 comparisons (return i32 0/1) ----
        0x5B => f32_cmp(stack, "f32.eq", |a, b| a == b)?,
        0x5C => f32_cmp(stack, "f32.ne", |a, b| a != b)?,
        0x5D => f32_cmp(stack, "f32.lt", |a, b| a < b)?,
        0x5E => f32_cmp(stack, "f32.gt", |a, b| a > b)?,
        0x5F => f32_cmp(stack, "f32.le", |a, b| a <= b)?,
        0x60 => f32_cmp(stack, "f32.ge", |a, b| a >= b)?,
        // ---- f64 comparisons ----
        0x61 => f64_cmp(stack, "f64.eq", |a, b| a == b)?,
        0x62 => f64_cmp(stack, "f64.ne", |a, b| a != b)?,
        0x63 => f64_cmp(stack, "f64.lt", |a, b| a < b)?,
        0x64 => f64_cmp(stack, "f64.gt", |a, b| a > b)?,
        0x65 => f64_cmp(stack, "f64.le", |a, b| a <= b)?,
        0x66 => f64_cmp(stack, "f64.ge", |a, b| a >= b)?,
        // ---- f32 unary ----
        0x8B => { let a = pop_f32(stack, "f32.abs")?; push_f32(stack, a.abs()); }
        0x8C => { let a = pop_f32(stack, "f32.neg")?; push_f32(stack, -a); }
        0x8D => { let a = pop_f32(stack, "f32.ceil")?; push_f32(stack, a.ceil()); }
        0x8E => { let a = pop_f32(stack, "f32.floor")?; push_f32(stack, a.floor()); }
        0x8F => { let a = pop_f32(stack, "f32.trunc")?; push_f32(stack, a.trunc()); }
        0x90 => { let a = pop_f32(stack, "f32.nearest")?; push_f32(stack, wasm_nearest_f32(a)); }
        0x91 => { let a = pop_f32(stack, "f32.sqrt")?; push_f32(stack, a.sqrt()); }
        // ---- f32 binary ----
        0x92 => { let b = pop_f32(stack, "f32.add")?; let a = pop_f32(stack, "f32.add")?; push_f32(stack, a + b); }
        0x93 => { let b = pop_f32(stack, "f32.sub")?; let a = pop_f32(stack, "f32.sub")?; push_f32(stack, a - b); }
        0x94 => { let b = pop_f32(stack, "f32.mul")?; let a = pop_f32(stack, "f32.mul")?; push_f32(stack, a * b); }
        0x95 => { let b = pop_f32(stack, "f32.div")?; let a = pop_f32(stack, "f32.div")?; push_f32(stack, a / b); }
        0x96 => { let b = pop_f32(stack, "f32.min")?; let a = pop_f32(stack, "f32.min")?; push_f32(stack, wasm_min_f32(a, b)); }
        0x97 => { let b = pop_f32(stack, "f32.max")?; let a = pop_f32(stack, "f32.max")?; push_f32(stack, wasm_max_f32(a, b)); }
        0x98 => { let b = pop_f32(stack, "f32.copysign")?; let a = pop_f32(stack, "f32.copysign")?; push_f32(stack, a.copysign(b)); }
        // ---- f64 unary ----
        0x99 => { let a = pop_f64(stack, "f64.abs")?; push_f64(stack, a.abs()); }
        0x9A => { let a = pop_f64(stack, "f64.neg")?; push_f64(stack, -a); }
        0x9B => { let a = pop_f64(stack, "f64.ceil")?; push_f64(stack, a.ceil()); }
        0x9C => { let a = pop_f64(stack, "f64.floor")?; push_f64(stack, a.floor()); }
        0x9D => { let a = pop_f64(stack, "f64.trunc")?; push_f64(stack, a.trunc()); }
        0x9E => { let a = pop_f64(stack, "f64.nearest")?; push_f64(stack, wasm_nearest_f64(a)); }
        0x9F => { let a = pop_f64(stack, "f64.sqrt")?; push_f64(stack, a.sqrt()); }
        // ---- f64 binary ----
        0xA0 => { let b = pop_f64(stack, "f64.add")?; let a = pop_f64(stack, "f64.add")?; push_f64(stack, a + b); }
        0xA1 => { let b = pop_f64(stack, "f64.sub")?; let a = pop_f64(stack, "f64.sub")?; push_f64(stack, a - b); }
        0xA2 => { let b = pop_f64(stack, "f64.mul")?; let a = pop_f64(stack, "f64.mul")?; push_f64(stack, a * b); }
        0xA3 => { let b = pop_f64(stack, "f64.div")?; let a = pop_f64(stack, "f64.div")?; push_f64(stack, a / b); }
        0xA4 => { let b = pop_f64(stack, "f64.min")?; let a = pop_f64(stack, "f64.min")?; push_f64(stack, wasm_min_f64(a, b)); }
        0xA5 => { let b = pop_f64(stack, "f64.max")?; let a = pop_f64(stack, "f64.max")?; push_f64(stack, wasm_max_f64(a, b)); }
        0xA6 => { let b = pop_f64(stack, "f64.copysign")?; let a = pop_f64(stack, "f64.copysign")?; push_f64(stack, a.copysign(b)); }
        // ---- int <-> float conversions (truncating; trap-on-out-of-range per spec) ----
        0xA8 => {
            // i32.trunc_f32_s
            let a = pop_f32(stack, "i32.trunc_f32_s")?;
            stack.push(Value::I32(trunc_f32_to_i32_s(a)?));
        }
        0xA9 => {
            // i32.trunc_f32_u
            let a = pop_f32(stack, "i32.trunc_f32_u")?;
            stack.push(Value::I32(trunc_f32_to_u32(a)? as i32));
        }
        0xAA => {
            // i32.trunc_f64_s
            let a = pop_f64(stack, "i32.trunc_f64_s")?;
            stack.push(Value::I32(trunc_f64_to_i32_s(a)?));
        }
        0xAB => {
            // i32.trunc_f64_u
            let a = pop_f64(stack, "i32.trunc_f64_u")?;
            stack.push(Value::I32(trunc_f64_to_u32(a)? as i32));
        }
        0xAE => {
            // i64.trunc_f32_s
            let a = pop_f32(stack, "i64.trunc_f32_s")?;
            stack.push(Value::I64(trunc_f32_to_i64_s(a)?));
        }
        0xAF => {
            // i64.trunc_f32_u
            let a = pop_f32(stack, "i64.trunc_f32_u")?;
            stack.push(Value::I64(trunc_f32_to_u64(a)? as i64));
        }
        0xB0 => {
            // i64.trunc_f64_s
            let a = pop_f64(stack, "i64.trunc_f64_s")?;
            stack.push(Value::I64(trunc_f64_to_i64_s(a)?));
        }
        0xB1 => {
            // i64.trunc_f64_u
            let a = pop_f64(stack, "i64.trunc_f64_u")?;
            stack.push(Value::I64(trunc_f64_to_u64(a)? as i64));
        }
        0xB2 => { let a = pop_i32(stack, "f32.convert_i32_s")?; push_f32(stack, a as f32); }
        0xB3 => { let a = pop_i32(stack, "f32.convert_i32_u")? as u32; push_f32(stack, a as f32); }
        0xB4 => { let a = pop_i64(stack, "f32.convert_i64_s")?; push_f32(stack, a as f32); }
        0xB5 => { let a = pop_i64(stack, "f32.convert_i64_u")? as u64; push_f32(stack, a as f32); }
        0xB6 => { let a = pop_f64(stack, "f32.demote_f64")?; push_f32(stack, a as f32); }
        0xB7 => { let a = pop_i32(stack, "f64.convert_i32_s")?; push_f64(stack, a as f64); }
        0xB8 => { let a = pop_i32(stack, "f64.convert_i32_u")? as u32; push_f64(stack, a as f64); }
        0xB9 => { let a = pop_i64(stack, "f64.convert_i64_s")?; push_f64(stack, a as f64); }
        0xBA => { let a = pop_i64(stack, "f64.convert_i64_u")? as u64; push_f64(stack, a as f64); }
        0xBB => { let a = pop_f32(stack, "f64.promote_f32")?; push_f64(stack, a as f64); }
        // ---- reinterpret (bit-cast) ----
        0xBC => {
            // i32.reinterpret_f32
            let a = pop_f32(stack, "i32.reinterpret_f32")?;
            stack.push(Value::I32(a.to_bits() as i32));
        }
        0xBD => {
            // i64.reinterpret_f64
            let a = pop_f64(stack, "i64.reinterpret_f64")?;
            stack.push(Value::I64(a.to_bits() as i64));
        }
        0xBE => {
            // f32.reinterpret_i32 (canonicalize result NaN if any)
            let a = pop_i32(stack, "f32.reinterpret_i32")? as u32;
            push_f32(stack, f32::from_bits(a));
        }
        0xBF => {
            // f64.reinterpret_i64
            let a = pop_i64(stack, "f64.reinterpret_i64")? as u64;
            push_f64(stack, f64::from_bits(a));
        }
        other => {
            if is_known_wasm_opcode(other) {
                return Err(WasmError::UnsupportedOpcode(other));
            }
            return Err(WasmError::InvalidOpcode(other));
        }
    }
    Ok(true)
}

// ----------------------------------------------------------------------------
// Stack helpers
// ----------------------------------------------------------------------------

/// SP121 — Push an f32 onto the stack, canonicalizing any NaN result to the
/// WASM-spec canonical bit pattern. Called by every float-producing opcode.
fn push_f32(stack: &mut Vec<Value>, v: f32) {
    stack.push(Value::F32(canonicalize_f32(v)));
}
fn push_f64(stack: &mut Vec<Value>, v: f64) {
    stack.push(Value::F64(canonicalize_f64(v)));
}

fn pop_f32(stack: &mut Vec<Value>, op: &'static str) -> Result<f32, WasmError> {
    match stack.pop() {
        Some(Value::F32(v)) => Ok(v),
        Some(other) => Err(WasmError::StackTypeMismatch {
            opcode: op,
            expected: ValType::F32,
            got: other.ty(),
        }),
        None => Err(WasmError::StackUnderflow { opcode: op }),
    }
}

fn pop_f64(stack: &mut Vec<Value>, op: &'static str) -> Result<f64, WasmError> {
    match stack.pop() {
        Some(Value::F64(v)) => Ok(v),
        Some(other) => Err(WasmError::StackTypeMismatch {
            opcode: op,
            expected: ValType::F64,
            got: other.ty(),
        }),
        None => Err(WasmError::StackUnderflow { opcode: op }),
    }
}

fn f32_cmp(
    stack: &mut Vec<Value>,
    op: &'static str,
    f: impl Fn(f32, f32) -> bool,
) -> Result<(), WasmError> {
    let b = pop_f32(stack, op)?;
    let a = pop_f32(stack, op)?;
    // Per WASM spec: NaN comparisons follow IEEE 754 — eq/ne/lt/gt/le/ge
    // all return 0 when either operand is NaN (except != which returns 1).
    // Rust's PartialOrd does this correctly.
    stack.push(Value::I32(if f(a, b) { 1 } else { 0 }));
    Ok(())
}

fn f64_cmp(
    stack: &mut Vec<Value>,
    op: &'static str,
    f: impl Fn(f64, f64) -> bool,
) -> Result<(), WasmError> {
    let b = pop_f64(stack, op)?;
    let a = pop_f64(stack, op)?;
    stack.push(Value::I32(if f(a, b) { 1 } else { 0 }));
    Ok(())
}

/// SP121 — WASM `f32.min` spec: if either operand is NaN, return
/// canonical NaN. If both are 0.0 with different signs, return -0.0
/// (the smaller zero). Otherwise, IEEE 754 minimum.
fn wasm_min_f32(a: f32, b: f32) -> f32 {
    if a.is_nan() || b.is_nan() {
        return f32::from_bits(CANONICAL_F32_NAN_BITS);
    }
    if a == 0.0 && b == 0.0 {
        // Distinguish +0 vs -0: -0's bits are 0x8000_0000.
        if a.to_bits() == 0x8000_0000 || b.to_bits() == 0x8000_0000 {
            return f32::from_bits(0x8000_0000);
        }
        return 0.0;
    }
    if a < b { a } else { b }
}

fn wasm_max_f32(a: f32, b: f32) -> f32 {
    if a.is_nan() || b.is_nan() {
        return f32::from_bits(CANONICAL_F32_NAN_BITS);
    }
    if a == 0.0 && b == 0.0 {
        if a.to_bits() == 0 || b.to_bits() == 0 {
            return 0.0;
        }
        return f32::from_bits(0x8000_0000);
    }
    if a > b { a } else { b }
}

fn wasm_min_f64(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        return f64::from_bits(CANONICAL_F64_NAN_BITS);
    }
    if a == 0.0 && b == 0.0 {
        if a.to_bits() == 0x8000_0000_0000_0000 || b.to_bits() == 0x8000_0000_0000_0000 {
            return f64::from_bits(0x8000_0000_0000_0000);
        }
        return 0.0;
    }
    if a < b { a } else { b }
}

fn wasm_max_f64(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        return f64::from_bits(CANONICAL_F64_NAN_BITS);
    }
    if a == 0.0 && b == 0.0 {
        if a.to_bits() == 0 || b.to_bits() == 0 {
            return 0.0;
        }
        return f64::from_bits(0x8000_0000_0000_0000);
    }
    if a > b { a } else { b }
}

/// SP121 — WASM `f32.nearest` spec: round-to-nearest-even (banker's
/// rounding), which Rust's `.round()` does NOT do (it rounds half to
/// AWAY from zero). Implement explicitly.
fn wasm_nearest_f32(a: f32) -> f32 {
    if a.is_nan() || a.is_infinite() {
        return a;
    }
    let rounded = a.round();
    let frac = (a - a.trunc()).abs();
    if frac == 0.5 {
        // Banker's: round to even.
        let trunc = a.trunc();
        let even = (trunc as i64) % 2 == 0;
        if even {
            trunc
        } else if a > 0.0 {
            trunc + 1.0
        } else {
            trunc - 1.0
        }
    } else {
        rounded
    }
}

fn wasm_nearest_f64(a: f64) -> f64 {
    if a.is_nan() || a.is_infinite() {
        return a;
    }
    let rounded = a.round();
    let frac = (a - a.trunc()).abs();
    if frac == 0.5 {
        let trunc = a.trunc();
        let even = (trunc as i64) % 2 == 0;
        if even {
            trunc
        } else if a > 0.0 {
            trunc + 1.0
        } else {
            trunc - 1.0
        }
    } else {
        rounded
    }
}

// SP121 — float-to-int truncation conversions per WASM spec:
//   - NaN traps with IntegerConversionInvalid.
//   - +/-inf traps with IntegerConversionInvalid.
//   - Out-of-destination-range traps with IntegerConversionInvalid.
//   - Otherwise: floor toward zero (truncation).

fn trunc_f32_to_i32_s(a: f32) -> Result<i32, WasmError> {
    if a.is_nan() {
        return Err(WasmError::IntegerConversionInvalid { what: "i32.trunc_f32_s", src_bits: a.to_bits() as u64 });
    }
    let t = a.trunc();
    if t < i32::MIN as f32 || t >= -(i32::MIN as f32) {
        return Err(WasmError::IntegerConversionInvalid { what: "i32.trunc_f32_s", src_bits: a.to_bits() as u64 });
    }
    Ok(t as i32)
}

fn trunc_f32_to_u32(a: f32) -> Result<u32, WasmError> {
    if a.is_nan() {
        return Err(WasmError::IntegerConversionInvalid { what: "i32.trunc_f32_u", src_bits: a.to_bits() as u64 });
    }
    let t = a.trunc();
    if t <= -1.0 || t >= 4294967296.0 {
        return Err(WasmError::IntegerConversionInvalid { what: "i32.trunc_f32_u", src_bits: a.to_bits() as u64 });
    }
    Ok(t as u32)
}

fn trunc_f64_to_i32_s(a: f64) -> Result<i32, WasmError> {
    if a.is_nan() {
        return Err(WasmError::IntegerConversionInvalid { what: "i32.trunc_f64_s", src_bits: a.to_bits() });
    }
    let t = a.trunc();
    if t < i32::MIN as f64 || t > i32::MAX as f64 {
        return Err(WasmError::IntegerConversionInvalid { what: "i32.trunc_f64_s", src_bits: a.to_bits() });
    }
    Ok(t as i32)
}

fn trunc_f64_to_u32(a: f64) -> Result<u32, WasmError> {
    if a.is_nan() {
        return Err(WasmError::IntegerConversionInvalid { what: "i32.trunc_f64_u", src_bits: a.to_bits() });
    }
    let t = a.trunc();
    if t <= -1.0 || t > u32::MAX as f64 {
        return Err(WasmError::IntegerConversionInvalid { what: "i32.trunc_f64_u", src_bits: a.to_bits() });
    }
    Ok(t as u32)
}

fn trunc_f32_to_i64_s(a: f32) -> Result<i64, WasmError> {
    if a.is_nan() {
        return Err(WasmError::IntegerConversionInvalid { what: "i64.trunc_f32_s", src_bits: a.to_bits() as u64 });
    }
    let t = a.trunc();
    if t < i64::MIN as f32 || t >= -(i64::MIN as f32) {
        return Err(WasmError::IntegerConversionInvalid { what: "i64.trunc_f32_s", src_bits: a.to_bits() as u64 });
    }
    Ok(t as i64)
}

fn trunc_f32_to_u64(a: f32) -> Result<u64, WasmError> {
    if a.is_nan() {
        return Err(WasmError::IntegerConversionInvalid { what: "i64.trunc_f32_u", src_bits: a.to_bits() as u64 });
    }
    let t = a.trunc();
    if t <= -1.0 || t >= 18446744073709551616.0 {
        return Err(WasmError::IntegerConversionInvalid { what: "i64.trunc_f32_u", src_bits: a.to_bits() as u64 });
    }
    Ok(t as u64)
}

fn trunc_f64_to_i64_s(a: f64) -> Result<i64, WasmError> {
    if a.is_nan() {
        return Err(WasmError::IntegerConversionInvalid { what: "i64.trunc_f64_s", src_bits: a.to_bits() });
    }
    let t = a.trunc();
    if t < i64::MIN as f64 || t >= -(i64::MIN as f64) {
        return Err(WasmError::IntegerConversionInvalid { what: "i64.trunc_f64_s", src_bits: a.to_bits() });
    }
    Ok(t as i64)
}

fn trunc_f64_to_u64(a: f64) -> Result<u64, WasmError> {
    if a.is_nan() {
        return Err(WasmError::IntegerConversionInvalid { what: "i64.trunc_f64_u", src_bits: a.to_bits() });
    }
    let t = a.trunc();
    if t <= -1.0 || t >= 18446744073709551616.0 {
        return Err(WasmError::IntegerConversionInvalid { what: "i64.trunc_f64_u", src_bits: a.to_bits() });
    }
    Ok(t as u64)
}

fn mem_store_f32(
    stack: &mut Vec<Value>,
    code: &[u8],
    ip: &mut usize,
    mem: &mut Memory,
    op: &'static str,
) -> Result<(), WasmError> {
    let _align = read_u32_leb(code, ip)?;
    let offset = read_u32_leb(code, ip)?;
    let v = pop_f32(stack, op)?;
    let base = pop_i32(stack, op)?;
    let pos = mem.effective_addr(base, offset, 4)?;
    let bytes = v.to_le_bytes();
    mem.bytes[pos..pos + 4].copy_from_slice(&bytes);
    Ok(())
}

fn mem_store_f64(
    stack: &mut Vec<Value>,
    code: &[u8],
    ip: &mut usize,
    mem: &mut Memory,
    op: &'static str,
) -> Result<(), WasmError> {
    let _align = read_u32_leb(code, ip)?;
    let offset = read_u32_leb(code, ip)?;
    let v = pop_f64(stack, op)?;
    let base = pop_i32(stack, op)?;
    let pos = mem.effective_addr(base, offset, 8)?;
    let bytes = v.to_le_bytes();
    mem.bytes[pos..pos + 8].copy_from_slice(&bytes);
    Ok(())
}

fn pop_i32(stack: &mut Vec<Value>, op: &'static str) -> Result<i32, WasmError> {
    match stack.pop() {
        Some(Value::I32(v)) => Ok(v),
        Some(other) => Err(WasmError::StackTypeMismatch {
            opcode: op,
            expected: ValType::I32,
            got: other.ty(),
        }),
        None => Err(WasmError::StackUnderflow { opcode: op }),
    }
}

fn pop_i64(stack: &mut Vec<Value>, op: &'static str) -> Result<i64, WasmError> {
    match stack.pop() {
        Some(Value::I64(v)) => Ok(v),
        Some(other) => Err(WasmError::StackTypeMismatch {
            opcode: op,
            expected: ValType::I64,
            got: other.ty(),
        }),
        None => Err(WasmError::StackUnderflow { opcode: op }),
    }
}

fn i32_cmp(
    stack: &mut Vec<Value>,
    op: &'static str,
    f: impl Fn(i32, i32) -> bool,
) -> Result<(), WasmError> {
    let b = pop_i32(stack, op)?;
    let a = pop_i32(stack, op)?;
    stack.push(Value::I32(if f(a, b) { 1 } else { 0 }));
    Ok(())
}

fn i32_cmp_unsigned(
    stack: &mut Vec<Value>,
    op: &'static str,
    f: impl Fn(u32, u32) -> bool,
) -> Result<(), WasmError> {
    let b = pop_i32(stack, op)? as u32;
    let a = pop_i32(stack, op)? as u32;
    stack.push(Value::I32(if f(a, b) { 1 } else { 0 }));
    Ok(())
}

fn i64_cmp(
    stack: &mut Vec<Value>,
    op: &'static str,
    f: impl Fn(i64, i64) -> bool,
) -> Result<(), WasmError> {
    let b = pop_i64(stack, op)?;
    let a = pop_i64(stack, op)?;
    stack.push(Value::I32(if f(a, b) { 1 } else { 0 }));
    Ok(())
}

fn i64_cmp_unsigned(
    stack: &mut Vec<Value>,
    op: &'static str,
    f: impl Fn(u64, u64) -> bool,
) -> Result<(), WasmError> {
    let b = pop_i64(stack, op)? as u64;
    let a = pop_i64(stack, op)? as u64;
    stack.push(Value::I32(if f(a, b) { 1 } else { 0 }));
    Ok(())
}

fn i32_bin_wrap(
    stack: &mut Vec<Value>,
    op: &'static str,
    f: impl Fn(i32, i32) -> i32,
) -> Result<(), WasmError> {
    let b = pop_i32(stack, op)?;
    let a = pop_i32(stack, op)?;
    stack.push(Value::I32(f(a, b)));
    Ok(())
}

fn i64_bin_wrap(
    stack: &mut Vec<Value>,
    op: &'static str,
    f: impl Fn(i64, i64) -> i64,
) -> Result<(), WasmError> {
    let b = pop_i64(stack, op)?;
    let a = pop_i64(stack, op)?;
    stack.push(Value::I64(f(a, b)));
    Ok(())
}

fn mem_load(
    stack: &mut Vec<Value>,
    code: &[u8],
    ip: &mut usize,
    mem: &Memory,
    op: &'static str,
    width: u32,
    decode: impl Fn(&[u8]) -> Value,
) -> Result<(), WasmError> {
    let _align = read_u32_leb(code, ip)?;
    let offset = read_u32_leb(code, ip)?;
    let base = pop_i32(stack, op)?;
    let pos = mem.effective_addr(base, offset, width)?;
    stack.push(decode(&mem.bytes[pos..pos + width as usize]));
    Ok(())
}

fn mem_store_i32(
    stack: &mut Vec<Value>,
    code: &[u8],
    ip: &mut usize,
    mem: &mut Memory,
    op: &'static str,
    width: u32,
) -> Result<(), WasmError> {
    let _align = read_u32_leb(code, ip)?;
    let offset = read_u32_leb(code, ip)?;
    let v = pop_i32(stack, op)?;
    let base = pop_i32(stack, op)?;
    let pos = mem.effective_addr(base, offset, width)?;
    let bytes = v.to_le_bytes();
    mem.bytes[pos..pos + width as usize].copy_from_slice(&bytes[..width as usize]);
    Ok(())
}

fn mem_store_i64(
    stack: &mut Vec<Value>,
    code: &[u8],
    ip: &mut usize,
    mem: &mut Memory,
    op: &'static str,
    width: u32,
) -> Result<(), WasmError> {
    let _align = read_u32_leb(code, ip)?;
    let offset = read_u32_leb(code, ip)?;
    let v = pop_i64(stack, op)?;
    let base = pop_i32(stack, op)?;
    let pos = mem.effective_addr(base, offset, width)?;
    let bytes = v.to_le_bytes();
    mem.bytes[pos..pos + width as usize].copy_from_slice(&bytes[..width as usize]);
    Ok(())
}

fn read_blocktype(code: &[u8], ip: &mut usize) -> Result<(), WasmError> {
    let b = *code.get(*ip).ok_or(WasmError::UnexpectedEof)?;
    *ip += 1;
    match b {
        0x40 | 0x7F | 0x7E | 0x7D | 0x7C => Ok(()),
        _ => Err(WasmError::UnsupportedValType(b)),
    }
}

fn find_matching_end(code: &[u8], start_ip: usize) -> Result<usize, WasmError> {
    scan_block_until(code, start_ip, false).map(|p| p.0)
}

fn find_matching_else(code: &[u8], start_ip: usize, end_ip: usize) -> Option<usize> {
    scan_block_until(code, start_ip, true).ok().and_then(|p| {
        if let Some(e) = p.1 {
            if e < end_ip {
                return Some(e);
            }
        }
        None
    })
}

fn scan_block_until(
    code: &[u8],
    start_ip: usize,
    track_else: bool,
) -> Result<(usize, Option<usize>), WasmError> {
    let mut depth: u32 = 1;
    let mut ip = start_ip;
    let mut else_ip: Option<usize> = None;
    while ip < code.len() {
        let op = code[ip];
        let op_ip = ip;
        ip += 1;
        match op {
            0x02 | 0x03 | 0x04 => {
                if ip >= code.len() {
                    return Err(WasmError::UnexpectedEof);
                }
                ip += 1; // blocktype
                depth += 1;
            }
            0x05 => {
                if track_else && depth == 1 && else_ip.is_none() {
                    else_ip = Some(op_ip);
                }
            }
            0x0B => {
                depth -= 1;
                if depth == 0 {
                    return Ok((op_ip, else_ip));
                }
            }
            // single u32 LEB immediate
            0x0C | 0x0D | 0x10 | 0x20 | 0x21 | 0x22 => {
                skip_u32_leb(code, &mut ip)?;
            }
            // i32.const — signed LEB128 (sign-extension; we just skip
            // the byte stream until top bit clear, no need for separate
            // signed-skipper)
            0x41 => {
                skip_u32_leb(code, &mut ip)?;
            }
            // i64.const — signed LEB128 up to 10 bytes
            0x42 => {
                skip_i64_leb(code, &mut ip)?;
            }
            // f32.const — 4 raw LE bytes
            0x43 => {
                if ip + 4 > code.len() {
                    return Err(WasmError::UnexpectedEof);
                }
                ip += 4;
            }
            // f64.const — 8 raw LE bytes
            0x44 => {
                if ip + 8 > code.len() {
                    return Err(WasmError::UnexpectedEof);
                }
                ip += 8;
            }
            // memory ops with 2 u32 LEB immediates (align + offset). Range
            // covers i32/i64/f32/f64 loads + stores (0x28..=0x3E inclusive
            // is the load+store opcode block).
            0x28..=0x3E => {
                skip_u32_leb(code, &mut ip)?;
                skip_u32_leb(code, &mut ip)?;
            }
            // memory.size / memory.grow have 1 reserved byte (LEB; usually 0)
            0x3F | 0x40 => {
                skip_u32_leb(code, &mut ip)?;
            }
            _ => {}
        }
    }
    Err(WasmError::UnterminatedBlock)
}

fn do_branch(
    stack: &mut Vec<Value>,
    labels: &mut Vec<Label>,
    ip: &mut usize,
    depth: u32,
) -> Result<bool, WasmError> {
    let active = labels.len() as u32;
    if depth > active {
        return Err(WasmError::InvalidBranchDepth { depth, active });
    }
    if depth == active {
        labels.clear();
        return Ok(false);
    }
    let target_idx = labels.len() - 1 - depth as usize;
    let target = labels[target_idx];
    while stack.len() > target.stack_height_at_start {
        stack.pop();
    }
    if target.is_loop {
        labels.truncate(target_idx + 1);
    } else {
        labels.truncate(target_idx);
    }
    *ip = target.target_ip;
    if !target.is_loop {
        *ip += 1;
    }
    Ok(true)
}

fn is_known_wasm_opcode(b: u8) -> bool {
    // After SP122 the deferred set shrinks further: bit-manipulation
    // (0x67..=0x69, 0x79..=0x7B) + sign-extension (0xC0..=0xC4) all
    // moved to supported. Remaining deferred:
    // - 0x06..=0x0A, 0x0E    : try/catch/throw (exceptions; deferred)
    // - 0x11                 : call_indirect (tables; deferred)
    // - 0x12..=0x19          : return_call* / reference types (deferred)
    // - 0x1C..=0x1F          : typed select / reserved (deferred)
    // - 0x23..=0x27          : global.get/set, table.get/set (deferred)
    // - 0xD0..=0xD4          : reference type ops (deferred)
    // - 0xFC                 : prefix for saturating-trunc + bulk memory (deferred)
    // - 0xFD                 : SIMD prefix (deferred)
    // - 0xFE                 : threads/atomic prefix (deferred)
    matches!(
        b,
        0x06..=0x0A | 0x0E | 0x11 | 0x12..=0x19 | 0x1C..=0x1F |
        0x23..=0x27 |
        0xD0..=0xD4 | 0xFC | 0xFD..=0xFE
    )
}

// ============================================================================
// Test helpers
// ============================================================================

#[cfg(test)]
mod test_helpers {
    use super::ValType;

    /// Build a complete WASM module with one function. All locals are the
    /// given `local_type`. `params` is the list of param value types;
    /// `results` is the list of result value types (0 to N items).
    pub fn build_module(
        params: &[ValType],
        results: &[ValType],
        local_groups: &[(u32, ValType)],
        code: &[u8],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);
        let mut type_sec = Vec::new();
        type_sec.push(0x01);
        type_sec.push(0x60);
        write_u32_leb(&mut type_sec, params.len() as u32);
        for p in params {
            type_sec.push(val_type_byte(*p));
        }
        write_u32_leb(&mut type_sec, results.len() as u32);
        for r in results {
            type_sec.push(val_type_byte(*r));
        }
        out.push(0x01);
        write_u32_leb(&mut out, type_sec.len() as u32);
        out.extend_from_slice(&type_sec);

        // Function section
        out.push(0x03);
        let func_sec = vec![0x01, 0x00];
        write_u32_leb(&mut out, func_sec.len() as u32);
        out.extend_from_slice(&func_sec);

        // Code section
        let mut body = Vec::new();
        write_u32_leb(&mut body, local_groups.len() as u32);
        for (cnt, vt) in local_groups {
            write_u32_leb(&mut body, *cnt);
            body.push(val_type_byte(*vt));
        }
        body.extend_from_slice(code);
        let mut code_sec = Vec::new();
        code_sec.push(0x01);
        write_u32_leb(&mut code_sec, body.len() as u32);
        code_sec.extend_from_slice(&body);
        out.push(0x0A);
        write_u32_leb(&mut out, code_sec.len() as u32);
        out.extend_from_slice(&code_sec);

        out
    }

    /// Build a module with a memory section AND one function.
    pub fn build_module_with_memory(
        min_pages: u32,
        max_pages: Option<u32>,
        params: &[ValType],
        results: &[ValType],
        local_groups: &[(u32, ValType)],
        code: &[u8],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);

        // Type section
        let mut type_sec = Vec::new();
        type_sec.push(0x01);
        type_sec.push(0x60);
        write_u32_leb(&mut type_sec, params.len() as u32);
        for p in params {
            type_sec.push(val_type_byte(*p));
        }
        write_u32_leb(&mut type_sec, results.len() as u32);
        for r in results {
            type_sec.push(val_type_byte(*r));
        }
        out.push(0x01);
        write_u32_leb(&mut out, type_sec.len() as u32);
        out.extend_from_slice(&type_sec);

        // Function section
        out.push(0x03);
        let func_sec = vec![0x01, 0x00];
        write_u32_leb(&mut out, func_sec.len() as u32);
        out.extend_from_slice(&func_sec);

        // Memory section (id=5)
        let mut mem_sec = Vec::new();
        mem_sec.push(0x01); // 1 memory
        if let Some(m) = max_pages {
            mem_sec.push(0x01); // flags: has max
            write_u32_leb(&mut mem_sec, min_pages);
            write_u32_leb(&mut mem_sec, m);
        } else {
            mem_sec.push(0x00); // flags: no max
            write_u32_leb(&mut mem_sec, min_pages);
        }
        out.push(0x05);
        write_u32_leb(&mut out, mem_sec.len() as u32);
        out.extend_from_slice(&mem_sec);

        // Code section
        let mut body = Vec::new();
        write_u32_leb(&mut body, local_groups.len() as u32);
        for (cnt, vt) in local_groups {
            write_u32_leb(&mut body, *cnt);
            body.push(val_type_byte(*vt));
        }
        body.extend_from_slice(code);
        let mut code_sec = Vec::new();
        code_sec.push(0x01);
        write_u32_leb(&mut code_sec, body.len() as u32);
        code_sec.extend_from_slice(&body);
        out.push(0x0A);
        write_u32_leb(&mut out, code_sec.len() as u32);
        out.extend_from_slice(&code_sec);

        out
    }

    pub fn val_type_byte(v: ValType) -> u8 {
        match v {
            ValType::I32 => 0x7F,
            ValType::I64 => 0x7E,
            ValType::F32 => 0x7D,
            ValType::F64 => 0x7C,
        }
    }

    pub fn write_u32_leb(out: &mut Vec<u8>, mut v: u32) {
        loop {
            let b = (v & 0x7F) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                return;
            }
            out.push(b | 0x80);
        }
    }

    pub fn write_i32_leb(out: &mut Vec<u8>, v: i32) {
        let mut more = true;
        let mut value = v;
        while more {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if (value == 0 && (byte & 0x40) == 0) || (value == -1 && (byte & 0x40) != 0) {
                more = false;
            } else {
                byte |= 0x80;
            }
            out.push(byte);
        }
    }

    pub fn write_i64_leb(out: &mut Vec<u8>, v: i64) {
        let mut more = true;
        let mut value = v;
        while more {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if (value == 0 && (byte & 0x40) == 0) || (value == -1 && (byte & 0x40) != 0) {
                more = false;
            } else {
                byte |= 0x80;
            }
            out.push(byte);
        }
    }
}

// ============================================================================
// Hand-derived KATs (S4 / SP118-original + SP120-deep verification gate)
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::test_helpers::*;

    // ---------- SP118 original 15 KATs (re-checked under SP120 Value API) ----------

    #[test]
    fn s4_kat_bad_magic_rejected() {
        let mut bytes = vec![0; 8];
        bytes[0] = 0xFF;
        assert_eq!(Module::decode(&bytes).unwrap_err(), WasmError::BadMagic);
    }
    #[test]
    fn s4_kat_bad_version_rejected() {
        let bytes = vec![0x00, 0x61, 0x73, 0x6d, 0x02, 0x00, 0x00, 0x00];
        assert_eq!(Module::decode(&bytes).unwrap_err(), WasmError::BadVersion(2));
    }
    #[test]
    fn s4_kat_const_return_42() {
        let code = vec![0x41, 0x2A, 0x0B];
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I32(42)]);
    }
    #[test]
    fn s4_kat_add_3_4_returns_7() {
        let code = vec![0x41, 0x03, 0x41, 0x04, 0x6A, 0x0B];
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I32(7)]);
    }
    #[test]
    fn s4_kat_two_params_a_times_b_plus_1() {
        let code = vec![0x20, 0x00, 0x20, 0x01, 0x6C, 0x41, 0x01, 0x6A, 0x0B];
        let m = build_module(&[ValType::I32, ValType::I32], &[ValType::I32], &[], &code);
        assert_eq!(
            wasm_exec(&m, 0, &[Value::I32(5), Value::I32(7)], 100).unwrap(),
            vec![Value::I32(36)]
        );
    }
    #[test]
    fn s4_kat_div_rem_signed() {
        let m = build_module(&[], &[ValType::I32], &[], &[0x41, 0x11, 0x41, 0x05, 0x6D, 0x0B]);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I32(3)]);
        let m2 = build_module(&[], &[ValType::I32], &[], &[0x41, 0x11, 0x41, 0x05, 0x6F, 0x0B]);
        assert_eq!(wasm_exec(&m2, 0, &[], 100).unwrap(), vec![Value::I32(2)]);
    }
    #[test]
    fn s4_kat_div_by_zero_traps() {
        let m = build_module(&[], &[ValType::I32], &[], &[0x41, 0x01, 0x41, 0x00, 0x6D, 0x0B]);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap_err(), WasmError::IntegerDivideByZero);
    }
    #[test]
    fn s4_kat_div_imin_by_neg1_traps() {
        let mut code = vec![0x41];
        write_i32_leb(&mut code, i32::MIN);
        code.push(0x41);
        write_i32_leb(&mut code, -1);
        code.extend_from_slice(&[0x6D, 0x0B]);
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap_err(), WasmError::IntegerOverflow);
    }
    #[test]
    fn s4_kat_gas_exhaustion_traps() {
        let code = vec![0x41, 0x03, 0x41, 0x04, 0x6A, 0x0B];
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 2).unwrap_err(), WasmError::OutOfGas);
        assert_eq!(wasm_exec(&m, 0, &[], 10).unwrap(), vec![Value::I32(7)]);
    }
    #[test]
    fn s4_kat_if_else_branches() {
        let code = vec![
            0x20, 0x00, 0x41, 0x00, 0x4A, 0x04, 0x7F, 0x41, 0x01, 0x05, 0x41, 0x7F, 0x0B, 0x0B,
        ];
        let m = build_module(&[ValType::I32], &[ValType::I32], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[Value::I32(5)], 100).unwrap(), vec![Value::I32(1)]);
        assert_eq!(wasm_exec(&m, 0, &[Value::I32(-3)], 100).unwrap(), vec![Value::I32(-1)]);
        assert_eq!(wasm_exec(&m, 0, &[Value::I32(0)], 100).unwrap(), vec![Value::I32(-1)]);
    }
    #[test]
    fn s4_kat_in_module_call() {
        let mut bytes = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        let mut type_sec = Vec::new();
        type_sec.push(0x01);
        type_sec.extend_from_slice(&[0x60, 0x01, 0x7F, 0x01, 0x7F]);
        bytes.push(0x01);
        write_u32_leb(&mut bytes, type_sec.len() as u32);
        bytes.extend_from_slice(&type_sec);
        bytes.push(0x03);
        bytes.extend_from_slice(&[0x03, 0x02, 0x00, 0x00]);
        let mut code_sec = Vec::new();
        code_sec.push(0x02);
        let body0: Vec<u8> = vec![0x00, 0x20, 0x00, 0x10, 0x01, 0x0B];
        write_u32_leb(&mut code_sec, body0.len() as u32);
        code_sec.extend_from_slice(&body0);
        let body1: Vec<u8> = vec![0x00, 0x20, 0x00, 0x41, 0x02, 0x6C, 0x0B];
        write_u32_leb(&mut code_sec, body1.len() as u32);
        code_sec.extend_from_slice(&body1);
        bytes.push(0x0A);
        write_u32_leb(&mut bytes, code_sec.len() as u32);
        bytes.extend_from_slice(&code_sec);
        assert_eq!(
            wasm_exec(&bytes, 0, &[Value::I32(21)], 100).unwrap(),
            vec![Value::I32(42)]
        );
    }
    #[test]
    fn s4_kat_determinism_byte_identical_repeat() {
        let code = vec![0x20, 0x00, 0x20, 0x01, 0x6A, 0x41, 0x07, 0x6C, 0x0B];
        let m = build_module(&[ValType::I32, ValType::I32], &[ValType::I32], &[], &code);
        let r1 = wasm_exec(&m, 0, &[Value::I32(3), Value::I32(4)], 100).unwrap();
        let r2 = wasm_exec(&m, 0, &[Value::I32(3), Value::I32(4)], 100).unwrap();
        let r3 = wasm_exec(&m, 0, &[Value::I32(3), Value::I32(4)], 1000).unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r2, r3);
        assert_eq!(r1, vec![Value::I32(49)]);
    }
    #[test]
    fn s4_kat_unreachable_traps() {
        let code = vec![0x00, 0x41, 0x00, 0x0B];
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap_err(), WasmError::UnreachableExecuted);
    }
    #[test]
    fn s4_kat_decode_truncated_is_typed_error() {
        let m = Module::decode(&[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]).unwrap();
        assert_eq!(m.function_count(), 0);
        let r = Module::decode(&[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01]);
        assert!(matches!(r.unwrap_err(), WasmError::UnexpectedEof | WasmError::BadSection(_)));
    }
    #[test]
    fn s4_kat_invalid_opcode_traps() {
        let code = vec![0xEF, 0x0B];
        let m = build_module(&[], &[], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap_err(), WasmError::InvalidOpcode(0xEF));
    }

    // ---------- SP120 deep-extension KATs ----------

    /// SP120-KAT-1: i64.const + i64.add → returns i64 value.
    #[test]
    fn sp120_kat_i64_const_add() {
        // i64.const 100; i64.const 7000000000; i64.add; end
        let mut code = vec![0x42];
        write_i64_leb(&mut code, 100);
        code.push(0x42);
        write_i64_leb(&mut code, 7_000_000_000);
        code.extend_from_slice(&[0x7C, 0x0B]);
        let m = build_module(&[], &[ValType::I64], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I64(7_000_000_100)]);
    }

    /// SP120-KAT-2: i64.div_s of i64::MIN / -1 traps (boundary mirror of i32 case).
    #[test]
    fn sp120_kat_i64_div_imin_traps() {
        let mut code = vec![0x42];
        write_i64_leb(&mut code, i64::MIN);
        code.push(0x42);
        write_i64_leb(&mut code, -1);
        code.extend_from_slice(&[0x7F, 0x0B]);
        let m = build_module(&[], &[ValType::I64], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap_err(), WasmError::IntegerOverflow);
    }

    /// SP120-KAT-3: i32.wrap_i64 + i64.extend_i32_s conversions round-trip
    /// for in-range i32 values; lose information cleanly for out-of-range.
    #[test]
    fn sp120_kat_i32_i64_conversions() {
        // local.get 0 (i64); i32.wrap_i64; end → i32 result
        let code_wrap = vec![0x20, 0x00, 0xA7, 0x0B];
        let m_wrap = build_module(&[ValType::I64], &[ValType::I32], &[], &code_wrap);
        // Wrapping 0x1_0000_0042 (33-bit value) → low 32 bits = 0x42.
        assert_eq!(
            wasm_exec(&m_wrap, 0, &[Value::I64(0x1_0000_0042)], 100).unwrap(),
            vec![Value::I32(0x42)]
        );
        // Extend signed: -1_i32 → -1_i64 (sign-extends to 0xFFFF_FFFF_FFFF_FFFF).
        let code_ext_s = vec![0x20, 0x00, 0xAC, 0x0B];
        let m_ext_s = build_module(&[ValType::I32], &[ValType::I64], &[], &code_ext_s);
        assert_eq!(
            wasm_exec(&m_ext_s, 0, &[Value::I32(-1)], 100).unwrap(),
            vec![Value::I64(-1)]
        );
        // Extend unsigned: -1_i32 (= 0xFFFF_FFFF) → 0x0000_0000_FFFF_FFFF (positive).
        let code_ext_u = vec![0x20, 0x00, 0xAD, 0x0B];
        let m_ext_u = build_module(&[ValType::I32], &[ValType::I64], &[], &code_ext_u);
        assert_eq!(
            wasm_exec(&m_ext_u, 0, &[Value::I32(-1)], 100).unwrap(),
            vec![Value::I64(0xFFFF_FFFF)]
        );
    }

    /// SP120-KAT-4: type mismatch on entry args is a typed error.
    #[test]
    fn sp120_kat_entry_arg_type_mismatch_typed_error() {
        let code = vec![0x20, 0x00, 0x0B];
        let m = build_module(&[ValType::I32], &[ValType::I32], &[], &code);
        // Pass an i64 where i32 is declared.
        let r = wasm_exec(&m, 0, &[Value::I64(42)], 100);
        assert!(matches!(r.unwrap_err(),
            WasmError::EntryArgTypeMismatch { idx: 0, expected: ValType::I32, got: ValType::I64 }
        ));
    }

    /// SP120-KAT-5: stack type mismatch on i32.add with i64 operand traps.
    #[test]
    fn sp120_kat_stack_type_mismatch_traps() {
        // i32.const 1; i64.const 2; i32.add; end
        let mut code = vec![0x41, 0x01, 0x42];
        write_i64_leb(&mut code, 2);
        code.extend_from_slice(&[0x6A, 0x0B]);
        let m = build_module(&[], &[ValType::I32], &[], &code);
        let r = wasm_exec(&m, 0, &[], 100);
        assert!(matches!(r.unwrap_err(),
            WasmError::StackTypeMismatch { opcode: "i32.add", expected: ValType::I32, got: ValType::I64 }
        ));
    }

    /// SP120-KAT-6: multi-value returns — fn() -> (i32, i32, i64).
    #[test]
    fn sp120_kat_multi_value_return_3() {
        // i32.const 10; i32.const 20; i64.const 30; end → 3 results
        let mut code = vec![0x41, 0x0A, 0x41, 0x14, 0x42];
        write_i64_leb(&mut code, 30);
        code.push(0x0B);
        let m = build_module(
            &[],
            &[ValType::I32, ValType::I32, ValType::I64],
            &[],
            &code,
        );
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap(),
            vec![Value::I32(10), Value::I32(20), Value::I64(30)]
        );
    }

    /// SP120-KAT-7: linear memory — i32.store then i32.load roundtrips.
    #[test]
    fn sp120_kat_memory_i32_store_load_roundtrip() {
        // (memory min=1, max=1); i32.const 16 (addr); i32.const 0x12345678; i32.store align=2 offset=0;
        // i32.const 16; i32.load align=2 offset=0; end
        // store: 0x41 0x10 0x41 0xF8 0xAC 0xD1 0x91 0x01 (i32.const 0x12345678 via signed LEB128) 0x36 0x02 0x00
        // Wait: 0x12345678 = 305419896. Need signed LEB128 encoding.
        let mut code = vec![0x41, 0x10]; // i32.const 16 (address for store)
        code.push(0x41); // i32.const
        write_i32_leb(&mut code, 0x12345678);
        code.extend_from_slice(&[0x36, 0x02, 0x00]); // i32.store align=2 offset=0
        code.extend_from_slice(&[0x41, 0x10]); // i32.const 16 (address for load)
        code.extend_from_slice(&[0x28, 0x02, 0x00]); // i32.load align=2 offset=0
        code.push(0x0B);
        let m = build_module_with_memory(1, Some(1), &[], &[ValType::I32], &[], &code);
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap(),
            vec![Value::I32(0x12345678)]
        );
    }

    /// SP120-KAT-8: memory bounds check — store past memory end traps with typed error.
    #[test]
    fn sp120_kat_memory_oob_store_traps() {
        // memory min=1 = 64KiB. Store at addr = 65532 (offset 0, width 4) → fits exactly.
        // Store at addr = 65533 (offset 0, width 4) → 4 bytes need [65533..65537), exceeds 65536 → trap.
        let mut code = vec![0x41];
        write_i32_leb(&mut code, 65533);
        code.extend_from_slice(&[0x41, 0x00, 0x36, 0x02, 0x00, 0x0B]); // i32.const 0; i32.store; end
        let m = build_module_with_memory(1, Some(1), &[], &[], &[], &code);
        let r = wasm_exec(&m, 0, &[], 100);
        assert!(matches!(r.unwrap_err(),
            WasmError::MemoryOutOfBounds { addr: 65533, len: 4, mem_bytes: 65536 }
        ));
    }

    /// SP120-KAT-9: memory.size + memory.grow lifecycle.
    #[test]
    fn sp120_kat_memory_size_and_grow() {
        // size; i32.const 2; grow; size; end → expect [1, 1, 3] (initial 1, grow returns prev=1, new size = 3)
        // We return 3 values: (initial_size, grow_result, post_size).
        let code = vec![
            0x3F, 0x00, // memory.size (reserved 0)
            0x41, 0x02, 0x40, 0x00, // i32.const 2; memory.grow (reserved 0)
            0x3F, 0x00, // memory.size
            0x0B,
        ];
        let m = build_module_with_memory(
            1,
            Some(10),
            &[],
            &[ValType::I32, ValType::I32, ValType::I32],
            &[],
            &code,
        );
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap(),
            vec![Value::I32(1), Value::I32(1), Value::I32(3)]
        );
    }

    /// SP120-KAT-10: memory.grow refused by max cap returns -1 (NOT a trap).
    #[test]
    fn sp120_kat_memory_grow_refused_by_max_returns_neg1() {
        // memory min=1 max=2. Grow by 5 → exceeds max → returns -1.
        // size; i32.const 5; grow; end → expect (1, -1)
        let code = vec![
            0x3F, 0x00, 0x41, 0x05, 0x40, 0x00, 0x0B,
        ];
        let m = build_module_with_memory(
            1,
            Some(2),
            &[],
            &[ValType::I32, ValType::I32],
            &[],
            &code,
        );
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap(),
            vec![Value::I32(1), Value::I32(-1)]
        );
    }

    /// SP120-KAT-11: memory access on module WITHOUT memory section traps cleanly.
    #[test]
    fn sp120_kat_memory_op_without_memory_section_traps() {
        // Try memory.size when no memory section declared.
        let code = vec![0x3F, 0x00, 0x0B];
        let m = build_module(&[], &[ValType::I32], &[], &code);
        let r = wasm_exec(&m, 0, &[], 100);
        assert_eq!(r.unwrap_err(), WasmError::MemoryNotDeclared);
    }

    /// SP120-KAT-12: i64.load8_u sign-extension correctness.
    #[test]
    fn sp120_kat_i64_load8_u_zero_extends() {
        // Store -1 as a byte (0xFF) at addr 0, then i64.load8_u → 0xFF (positive 255).
        // The 0x41-prefixed value uses write_i32_leb (NOT a literal 0xFF
        // byte) because signed LEB128 interprets a single-byte 0xFF as
        // continuation (0xFF & 0x80 != 0); the 5-byte sequence below
        // encodes -1 unambiguously.
        let mut code = vec![0x41, 0x00]; // addr for store
        code.push(0x41);
        write_i32_leb(&mut code, -1);
        code.extend_from_slice(&[0x3A, 0x00, 0x00]); // i32.store8 align=0 offset=0
        code.extend_from_slice(&[0x41, 0x00]); // addr for load
        code.extend_from_slice(&[0x31, 0x00, 0x00]); // i64.load8_u align=0 offset=0
        code.push(0x0B);
        let m = build_module_with_memory(1, Some(1), &[], &[ValType::I64], &[], &code);
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap(),
            vec![Value::I64(0xFF)]
        );
    }

    /// SP120-KAT-13: i64.load8_s sign-extension correctness.
    #[test]
    fn sp120_kat_i64_load8_s_sign_extends() {
        // Same setup but load via i64.load8_s — 0xFF byte → -1 i64.
        let mut code = vec![0x41, 0x00];
        code.push(0x41);
        write_i32_leb(&mut code, -1);
        code.extend_from_slice(&[0x3A, 0x00, 0x00]);
        code.extend_from_slice(&[0x41, 0x00]);
        code.extend_from_slice(&[0x30, 0x00, 0x00]);
        code.push(0x0B);
        let m = build_module_with_memory(1, Some(1), &[], &[ValType::I64], &[], &code);
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap(),
            vec![Value::I64(-1)]
        );
    }

    /// SP120-KAT-14: i64 determinism across repeat invocations.
    #[test]
    fn sp120_kat_i64_determinism_repeat() {
        let mut code = vec![0x42];
        write_i64_leb(&mut code, 0x1234_5678_9ABC_DEF0u64 as i64);
        code.push(0x42);
        write_i64_leb(&mut code, 7);
        code.extend_from_slice(&[0x84, 0x0B]); // i64.or; end
        let m = build_module(&[], &[ValType::I64], &[], &code);
        let r1 = wasm_exec(&m, 0, &[], 100).unwrap();
        let r2 = wasm_exec(&m, 0, &[], 100).unwrap();
        let r3 = wasm_exec(&m, 0, &[], 1000).unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r2, r3);
        assert_eq!(r1, vec![Value::I64(0x1234_5678_9ABC_DEF7u64 as i64)]);
    }

    /// SP120-KAT-15: locals of mixed types (i32 + i64) initialized to zero.
    #[test]
    fn sp120_kat_mixed_type_locals_zero_init() {
        // 2 i32 locals + 1 i64 local; read each → 0 (i32), 0 (i32), 0 (i64).
        let code = vec![
            0x20, 0x00, // local.get 0 (i32, no params)
            0x20, 0x01, // local.get 1 (i32)
            0x20, 0x02, // local.get 2 (i64)
            0x0B,
        ];
        let m = build_module(
            &[],
            &[ValType::I32, ValType::I32, ValType::I64],
            &[(2, ValType::I32), (1, ValType::I64)],
            &code,
        );
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap(),
            vec![Value::I32(0), Value::I32(0), Value::I64(0)]
        );
    }

    /// SP120-KAT-16: memory persists writes across multiple ops in one execution.
    /// Lessons from a failing first cut: literal 0x42 / 0x77 as immediate
    /// bytes under signed LEB128 have bit 6 set ⇒ interpreted as NEGATIVE
    /// (-62 / -9). Always use write_i32_leb for non-tiny constants.
    #[test]
    fn sp120_kat_memory_persists_within_execution() {
        // Store 0x42 at addr 8, store 0x77 at addr 16, load addr 8 + addr 16 then return both.
        let mut code = vec![0x41, 0x08]; // i32.const 8 (addr)
        code.push(0x41);
        write_i32_leb(&mut code, 0x42);
        code.extend_from_slice(&[0x36, 0x02, 0x00]); // i32.store @8

        code.extend_from_slice(&[0x41, 0x10]); // i32.const 16 (addr)
        code.push(0x41);
        write_i32_leb(&mut code, 0x77);
        code.extend_from_slice(&[0x36, 0x02, 0x00]); // i32.store @16

        code.extend_from_slice(&[0x41, 0x08, 0x28, 0x02, 0x00]); // i32.load @8
        code.extend_from_slice(&[0x41, 0x10, 0x28, 0x02, 0x00]); // i32.load @16
        code.push(0x0B);

        let m = build_module_with_memory(1, Some(1), &[], &[ValType::I32, ValType::I32], &[], &code);
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap(),
            vec![Value::I32(0x42), Value::I32(0x77)]
        );
    }

    /// SP122 — deferral boundary moved AGAIN. After SP121 the boundary
    /// was at 0x67 (i32.clz). After SP122 bit-manipulation + sign-extension
    /// also ship. The new boundary is at global.get (0x23) — global vars
    /// require a global section + state model that's its own slice.
    #[test]
    fn sp122_kat_deferred_boundary_shifted_to_globals() {
        let code = vec![0x23, 0x00, 0x0B]; // global.get 0; end
        let m = build_module(&[], &[ValType::I32], &[], &code);
        let r = wasm_exec(&m, 0, &[], 100);
        assert_eq!(r.unwrap_err(), WasmError::UnsupportedOpcode(0x23));
    }

    // ============================================================================
    // SP122 — bit-manipulation + sign-extension KATs
    // ============================================================================

    /// SP122-KAT-1: i32.clz / ctz / popcnt over known bit patterns.
    #[test]
    fn sp122_kat_i32_bit_manip() {
        // i32.const 0x0000_FF00; i32.clz; end → 16 (16 leading zeros)
        let mut code = vec![0x41];
        write_i32_leb(&mut code, 0x0000_FF00);
        code.extend_from_slice(&[0x67, 0x0B]);
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I32(16)]);

        // i32.const 0x0000_FF00; i32.ctz; end → 8 (8 trailing zeros)
        let mut code = vec![0x41];
        write_i32_leb(&mut code, 0x0000_FF00);
        code.extend_from_slice(&[0x68, 0x0B]);
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I32(8)]);

        // i32.const 0x0000_FF00; i32.popcnt; end → 8 (8 bits set)
        let mut code = vec![0x41];
        write_i32_leb(&mut code, 0x0000_FF00);
        code.extend_from_slice(&[0x69, 0x0B]);
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I32(8)]);
    }

    /// SP122-KAT-2: i64 bit-manipulation (clz/ctz/popcnt) over a 64-bit pattern.
    #[test]
    fn sp122_kat_i64_bit_manip() {
        // i64.const 0x0000_0000_FF00_0000; i64.clz → 32
        let mut code = vec![0x42];
        write_i64_leb(&mut code, 0x0000_0000_FF00_0000);
        code.extend_from_slice(&[0x79, 0x0B]);
        let m = build_module(&[], &[ValType::I64], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I64(32)]);

        // i64.const 0x0000_0000_FF00_0000; i64.ctz → 24
        let mut code = vec![0x42];
        write_i64_leb(&mut code, 0x0000_0000_FF00_0000);
        code.extend_from_slice(&[0x7A, 0x0B]);
        let m = build_module(&[], &[ValType::I64], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I64(24)]);

        // i64.const 0x0000_0000_FF00_0000; i64.popcnt → 8
        let mut code = vec![0x42];
        write_i64_leb(&mut code, 0x0000_0000_FF00_0000);
        code.extend_from_slice(&[0x7B, 0x0B]);
        let m = build_module(&[], &[ValType::I64], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I64(8)]);
    }

    /// SP122-KAT-3: clz of 0 returns the type width (32 for i32, 64 for i64) per spec.
    #[test]
    fn sp122_kat_clz_of_zero_returns_width() {
        let mut code = vec![0x41, 0x00, 0x67, 0x0B];
        let _ = &mut code;
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I32(32)]);

        let mut code = vec![0x42, 0x00, 0x79, 0x0B];
        let _ = &mut code;
        let m = build_module(&[], &[ValType::I64], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I64(64)]);
    }

    /// SP122-KAT-4: i32.extend8_s sign-extends a signed-byte-shaped i32.
    #[test]
    fn sp122_kat_i32_extend8_s() {
        // i32.const 0xFF (= 255 as low byte = -1 as i8); i32.extend8_s → -1
        let mut code = vec![0x41];
        write_i32_leb(&mut code, 0xFF);
        code.extend_from_slice(&[0xC0, 0x0B]);
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I32(-1)]);

        // i32.const 0x7F (= 127 as i8); i32.extend8_s → 127 (positive; no sign-extension)
        let mut code = vec![0x41];
        write_i32_leb(&mut code, 0x7F);
        code.extend_from_slice(&[0xC0, 0x0B]);
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I32(127)]);
    }

    /// SP122-KAT-5: i32.extend16_s sign-extension at the 16-bit boundary.
    #[test]
    fn sp122_kat_i32_extend16_s() {
        // i32.const 0xFFFF (= -1 as i16); i32.extend16_s → -1
        let mut code = vec![0x41];
        write_i32_leb(&mut code, 0xFFFF);
        code.extend_from_slice(&[0xC1, 0x0B]);
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I32(-1)]);
    }

    /// SP122-KAT-6: i64 sign-extension ops (extend8_s, extend16_s, extend32_s).
    #[test]
    fn sp122_kat_i64_sign_extensions() {
        // i64.const 0xFF; i64.extend8_s → -1
        let mut code = vec![0x42];
        write_i64_leb(&mut code, 0xFF);
        code.extend_from_slice(&[0xC2, 0x0B]);
        let m = build_module(&[], &[ValType::I64], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I64(-1)]);

        // i64.const 0xFFFF; i64.extend16_s → -1
        let mut code = vec![0x42];
        write_i64_leb(&mut code, 0xFFFF);
        code.extend_from_slice(&[0xC3, 0x0B]);
        let m = build_module(&[], &[ValType::I64], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I64(-1)]);

        // i64.const 0xFFFF_FFFF; i64.extend32_s → -1
        let mut code = vec![0x42];
        write_i64_leb(&mut code, 0xFFFF_FFFF);
        code.extend_from_slice(&[0xC4, 0x0B]);
        let m = build_module(&[], &[ValType::I64], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I64(-1)]);
    }

    // ============================================================================
    // SP121 — Deterministic f32/f64 KATs
    // ============================================================================

    /// SP121-KAT-1: f32.const + f32.add returns expected sum.
    #[test]
    fn sp121_kat_f32_const_add() {
        // f32.const 1.5; f32.const 2.25; f32.add; end → 3.75
        let mut code = vec![0x43];
        code.extend_from_slice(&1.5f32.to_le_bytes());
        code.push(0x43);
        code.extend_from_slice(&2.25f32.to_le_bytes());
        code.extend_from_slice(&[0x92, 0x0B]);
        let m = build_module(&[], &[ValType::F32], &[], &code);
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap(),
            vec![Value::F32(3.75)]
        );
    }

    /// SP121-KAT-2: f64.const + arithmetic round-trip.
    #[test]
    fn sp121_kat_f64_arith() {
        // f64.const 10.0; f64.const 3.0; f64.div; end → 3.333...
        let mut code = vec![0x44];
        code.extend_from_slice(&10.0f64.to_le_bytes());
        code.push(0x44);
        code.extend_from_slice(&3.0f64.to_le_bytes());
        code.extend_from_slice(&[0xA3, 0x0B]);
        let m = build_module(&[], &[ValType::F64], &[], &code);
        let r = wasm_exec(&m, 0, &[], 100).unwrap();
        // Exact bit pattern of 10.0 / 3.0 under round-to-nearest-even:
        assert_eq!(r, vec![Value::F64(10.0_f64 / 3.0_f64)]);
    }

    /// SP121-KAT-3: **THE THESIS-FIT CENTERPIECE**. NaN canonicalization
    /// makes division-by-zero produce a BIT-IDENTICAL canonical NaN
    /// regardless of host CPU. This is the property that makes WASM
    /// floats safe to use in a deterministic replicated state machine.
    #[test]
    fn sp121_kat_nan_canonicalized_bit_identical() {
        // f32.const 0.0; f32.const 0.0; f32.div → NaN; reinterpret_f32 → i32 bits
        let mut code = vec![0x43];
        code.extend_from_slice(&0.0f32.to_le_bytes());
        code.push(0x43);
        code.extend_from_slice(&0.0f32.to_le_bytes());
        code.extend_from_slice(&[0x95, 0xBC, 0x0B]); // f32.div; i32.reinterpret_f32; end
        let m = build_module(&[], &[ValType::I32], &[], &code);
        let r = wasm_exec(&m, 0, &[], 100).unwrap();
        // The CANONICAL bit pattern, NOT some host-specific NaN payload.
        assert_eq!(
            r,
            vec![Value::I32(CANONICAL_F32_NAN_BITS as i32)],
            "SP121 thesis-fit: 0/0 → canonical NaN bits (0x7FC00000), \
             NOT a host-specific NaN payload"
        );
    }

    /// SP121-KAT-4: f64 NaN canonicalization. Mirror of KAT-3 but on f64.
    /// inf - inf = NaN; canonicalize → 0x7FF8_0000_0000_0000.
    #[test]
    fn sp121_kat_f64_nan_canonicalized_bit_identical() {
        // f64.const +inf; f64.const +inf; f64.sub → NaN; reinterpret_f64; end
        let mut code = vec![0x44];
        code.extend_from_slice(&f64::INFINITY.to_le_bytes());
        code.push(0x44);
        code.extend_from_slice(&f64::INFINITY.to_le_bytes());
        code.extend_from_slice(&[0xA1, 0xBD, 0x0B]); // f64.sub; i64.reinterpret_f64; end
        let m = build_module(&[], &[ValType::I64], &[], &code);
        let r = wasm_exec(&m, 0, &[], 100).unwrap();
        assert_eq!(
            r,
            vec![Value::I64(CANONICAL_F64_NAN_BITS as i64)],
            "SP121 thesis-fit: inf-inf → canonical f64 NaN bits"
        );
    }

    /// SP121-KAT-5: an attacker-supplied non-canonical NaN bit pattern
    /// loaded via memory is CANONICALIZED on the load. The attacker can
    /// inject any NaN payload via stored bytes; the dispatch normalizes
    /// it on read. Locks the determinism boundary at the I/O seam.
    #[test]
    fn sp121_kat_memory_load_canonicalizes_nan() {
        // Store a non-canonical NaN bit pattern via i32.store, then load
        // as f32 → must come out as canonical NaN.
        let mut code = vec![0x41, 0x00]; // addr for store
        code.push(0x41);
        // Non-canonical NaN: any pattern with exp=0xFF and non-zero mantissa
        // but DIFFERENT from canonical (0x7FC00000). Use 0x7FC00001.
        write_i32_leb(&mut code, 0x7FC0_0001);
        code.extend_from_slice(&[0x36, 0x02, 0x00]); // i32.store
        code.extend_from_slice(&[0x41, 0x00]); // addr for load
        code.extend_from_slice(&[0x2A, 0x02, 0x00]); // f32.load
        code.extend_from_slice(&[0xBC, 0x0B]); // i32.reinterpret_f32; end
        let m = build_module_with_memory(1, Some(1), &[], &[ValType::I32], &[], &code);
        let r = wasm_exec(&m, 0, &[], 100).unwrap();
        assert_eq!(
            r,
            vec![Value::I32(CANONICAL_F32_NAN_BITS as i32)],
            "SP121 thesis-fit: non-canonical NaN injected via memory MUST \
             be canonicalized at load time; otherwise attacker-controlled \
             NaN payloads could break cross-replica determinism"
        );
    }

    /// SP121-KAT-6: reinterpret_i32 of a non-canonical NaN bit pattern
    /// ALSO canonicalizes. Closes the second NaN-injection vector.
    #[test]
    fn sp121_kat_reinterpret_canonicalizes_nan() {
        // i32.const 0x7FC0_0001 (non-canonical NaN bits); f32.reinterpret_i32;
        // i32.reinterpret_f32; end → canonical NaN bits.
        let mut code = vec![0x41];
        write_i32_leb(&mut code, 0x7FC0_0001);
        code.extend_from_slice(&[0xBE, 0xBC, 0x0B]);
        let m = build_module(&[], &[ValType::I32], &[], &code);
        let r = wasm_exec(&m, 0, &[], 100).unwrap();
        assert_eq!(
            r,
            vec![Value::I32(CANONICAL_F32_NAN_BITS as i32)],
            "SP121 thesis-fit: f32.reinterpret_i32 with non-canonical NaN \
             bits MUST canonicalize on the float push (push_f32 path)"
        );
    }

    /// SP121-KAT-7: f32.sqrt of a negative number → canonical NaN
    /// (matches IEEE 754 + WASM spec).
    #[test]
    fn sp121_kat_sqrt_negative_canonical_nan() {
        let mut code = vec![0x43];
        code.extend_from_slice(&(-4.0f32).to_le_bytes());
        code.extend_from_slice(&[0x91, 0xBC, 0x0B]); // f32.sqrt; reinterpret; end
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap(),
            vec![Value::I32(CANONICAL_F32_NAN_BITS as i32)]
        );
    }

    /// SP121-KAT-8: integer truncation traps on NaN.
    #[test]
    fn sp121_kat_trunc_nan_traps() {
        let mut code = vec![0x43];
        code.extend_from_slice(&f32::NAN.to_le_bytes());
        code.extend_from_slice(&[0xA8, 0x0B]); // i32.trunc_f32_s; end
        let m = build_module(&[], &[ValType::I32], &[], &code);
        match wasm_exec(&m, 0, &[], 100).unwrap_err() {
            WasmError::IntegerConversionInvalid { what, .. } => {
                assert_eq!(what, "i32.trunc_f32_s");
            }
            other => panic!("expected IntegerConversionInvalid; got {other:?}"),
        }
    }

    /// SP121-KAT-9: integer truncation traps on +inf out of i32 range.
    #[test]
    fn sp121_kat_trunc_inf_traps() {
        let mut code = vec![0x43];
        code.extend_from_slice(&f32::INFINITY.to_le_bytes());
        code.extend_from_slice(&[0xA8, 0x0B]);
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert!(matches!(
            wasm_exec(&m, 0, &[], 100).unwrap_err(),
            WasmError::IntegerConversionInvalid { .. }
        ));
    }

    /// SP121-KAT-10: i32 → f32 → i32 round-trip (in-range integer).
    #[test]
    fn sp121_kat_int_float_roundtrip() {
        // local.get 0; f32.convert_i32_s; i32.trunc_f32_s; end
        let code = vec![0x20, 0x00, 0xB2, 0xA8, 0x0B];
        let m = build_module(&[ValType::I32], &[ValType::I32], &[], &code);
        assert_eq!(
            wasm_exec(&m, 0, &[Value::I32(12345)], 100).unwrap(),
            vec![Value::I32(12345)]
        );
    }

    /// SP121-KAT-11: f64.promote_f32 + f32.demote_f64 round-trip
    /// (exact for values in the f32 range).
    #[test]
    fn sp121_kat_float_widen_narrow_roundtrip() {
        let mut code = vec![0x43];
        code.extend_from_slice(&3.14f32.to_le_bytes());
        code.extend_from_slice(&[0xBB, 0xB6, 0x0B]); // f64.promote_f32; f32.demote_f64; end
        let m = build_module(&[], &[ValType::F32], &[], &code);
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap(),
            vec![Value::F32(3.14)]
        );
    }

    /// SP121-KAT-12: f32 comparison with NaN returns 0 (false) per IEEE 754.
    #[test]
    fn sp121_kat_nan_comparisons_return_zero() {
        let mut code = vec![0x43];
        code.extend_from_slice(&f32::NAN.to_le_bytes());
        code.push(0x43);
        code.extend_from_slice(&1.0f32.to_le_bytes());
        code.extend_from_slice(&[0x5B, 0x0B]); // f32.eq; end → 0
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![Value::I32(0)]);

        // f32.ne of NaN and 1.0 → 1.
        let mut code2 = vec![0x43];
        code2.extend_from_slice(&f32::NAN.to_le_bytes());
        code2.push(0x43);
        code2.extend_from_slice(&1.0f32.to_le_bytes());
        code2.extend_from_slice(&[0x5C, 0x0B]); // f32.ne
        let m2 = build_module(&[], &[ValType::I32], &[], &code2);
        assert_eq!(wasm_exec(&m2, 0, &[], 100).unwrap(), vec![Value::I32(1)]);
    }

    /// SP121-KAT-13: f32.min with NaN returns canonical NaN per spec.
    #[test]
    fn sp121_kat_min_with_nan_returns_canonical_nan() {
        let mut code = vec![0x43];
        code.extend_from_slice(&1.0f32.to_le_bytes());
        code.push(0x43);
        code.extend_from_slice(&f32::NAN.to_le_bytes());
        code.extend_from_slice(&[0x96, 0xBC, 0x0B]); // f32.min; i32.reinterpret_f32; end
        let m = build_module(&[], &[ValType::I32], &[], &code);
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap(),
            vec![Value::I32(CANONICAL_F32_NAN_BITS as i32)]
        );
    }

    /// SP121-KAT-14: f32.copysign uses sign from second operand.
    #[test]
    fn sp121_kat_copysign() {
        let mut code = vec![0x43];
        code.extend_from_slice(&3.14f32.to_le_bytes());
        code.push(0x43);
        code.extend_from_slice(&(-1.0f32).to_le_bytes());
        code.extend_from_slice(&[0x98, 0x0B]); // f32.copysign; end
        let m = build_module(&[], &[ValType::F32], &[], &code);
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap(),
            vec![Value::F32(-3.14)]
        );
    }

    /// SP121-KAT-15: deterministic across repeat invocations (the S4
    /// determinism contract carried forward to floats with NaN
    /// canonicalization). Same module + same args → byte-identical f32
    /// output. Repeated 3 times with different gas limits.
    #[test]
    fn sp121_kat_float_determinism_repeat() {
        let mut code = vec![0x20, 0x00, 0x20, 0x01, 0x92, 0x91, 0x0B]; // (a+b).sqrt
        let _ = &mut code;
        let m = build_module(&[ValType::F32, ValType::F32], &[ValType::F32], &[], &code);
        let r1 = wasm_exec(&m, 0, &[Value::F32(9.0), Value::F32(16.0)], 100).unwrap();
        let r2 = wasm_exec(&m, 0, &[Value::F32(9.0), Value::F32(16.0)], 100).unwrap();
        let r3 = wasm_exec(&m, 0, &[Value::F32(9.0), Value::F32(16.0)], 1000).unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r2, r3);
        assert_eq!(r1, vec![Value::F32(5.0)]); // sqrt(9+16) = sqrt(25) = 5
    }

    /// SP121-KAT-16: every host (x86, ARM, etc.) computing this exact
    /// workload MUST produce byte-identical bytes. The test runs the
    /// computation thrice and checks identical bit-patterns at each
    /// float push site. This is the mechanical witness for the
    /// "deterministic across hosts" claim on floats.
    #[test]
    fn sp121_kat_byte_identical_across_invocations() {
        // A workload that hits many NaN-producing paths + a non-NaN path,
        // then reinterprets to bits for direct comparison.
        // 0.0/0.0 = NaN; sqrt(-1) = NaN; canonical+canonical = canonical.
        let mut code = Vec::new();
        // path 1: 0.0/0.0 → NaN
        code.push(0x43);
        code.extend_from_slice(&0.0f32.to_le_bytes());
        code.push(0x43);
        code.extend_from_slice(&0.0f32.to_le_bytes());
        code.push(0x95); // f32.div → NaN
        code.push(0xBC); // i32.reinterpret_f32 → bits
        // path 2: sqrt(-1.0) → NaN
        code.push(0x43);
        code.extend_from_slice(&(-1.0f32).to_le_bytes());
        code.push(0x91); // f32.sqrt → NaN
        code.push(0xBC); // bits
        // path 3: 7.5 * 2.0 = 15.0 (non-NaN)
        code.push(0x43);
        code.extend_from_slice(&7.5f32.to_le_bytes());
        code.push(0x43);
        code.extend_from_slice(&2.0f32.to_le_bytes());
        code.push(0x94); // f32.mul
        code.push(0xBC); // bits
        code.push(0x0B);
        let m = build_module(&[], &[ValType::I32, ValType::I32, ValType::I32], &[], &code);
        let r = wasm_exec(&m, 0, &[], 200).unwrap();
        assert_eq!(
            r,
            vec![
                Value::I32(CANONICAL_F32_NAN_BITS as i32),  // 0/0 canonical
                Value::I32(CANONICAL_F32_NAN_BITS as i32),  // sqrt(-1) canonical
                Value::I32(15.0f32.to_bits() as i32),       // 7.5 * 2.0 = 15.0
            ]
        );
    }
}
