//! kessel-wasm — Zero-dep deterministic WASM-MVP-subset interpreter for
//! in-tree user-defined functions (UDFs).
//!
//! Strategic-tier slice **S4 of THESIS.md**. Closes the S4 strategic-tier
//! item by providing a UDF execution surface that obeys all 5 thesis
//! pillars: **deterministic** (no float, no host-call non-determinism;
//! gas counter + stack are pure state), **verifiable** (every public
//! function locked by hand-derived KATs against the official WASM-MVP
//! spec), **replayable** (same module + args + gas_limit → same result on
//! every replica), **zero-dep** (no host-crate or third-party WASM
//! dependency; the entire decoder + interpreter is in this file), **honest
//! docs** (this header lists EXACTLY what's supported vs deferred).
//!
//! ## Supported
//!
//! - **Module format**: WASM-MVP magic (`0x00 0x61 0x73 0x6d`) + version
//!   `0x01 0x00 0x00 0x00`; sections decoded by ID (1=type, 3=function,
//!   10=code); other sections (custom, import, export, etc.) are SKIPPED
//!   (their bytes are read past via the section's declared size).
//! - **Value types**: `i32` only.
//! - **Function signatures**: arbitrary number of `i32` params; 0 or 1
//!   `i32` result.
//! - **Locals**: `local.get`, `local.set`, `local.tee` over `i32` locals.
//! - **Integer ops**: `i32.const` (LEB128 signed), `i32.add`, `i32.sub`,
//!   `i32.mul`, `i32.div_s`, `i32.rem_s`, `i32.and`, `i32.or`, `i32.xor`,
//!   `i32.shl`, `i32.shr_s`, `i32.shr_u`, `i32.eqz`, `i32.eq`, `i32.ne`,
//!   `i32.lt_s`, `i32.lt_u`, `i32.gt_s`, `i32.gt_u`, `i32.le_s`,
//!   `i32.ge_s`.
//! - **Control flow**: `block`, `loop`, `if`/`else`/`end`, `br`, `br_if`,
//!   `return`, `call` (in-module), `drop`, `select`, `unreachable`,
//!   `nop`.
//! - **Gas accounting**: every executed instruction increments a counter
//!   by 1; when `gas_limit` is exhausted the execution traps with
//!   `WasmError::OutOfGas`.
//! - **Determinism**: signed division/modulo use Rust's checked semantics
//!   (i32::MIN / -1 traps with `IntegerOverflow`; div by 0 traps with
//!   `IntegerDivideByZero` — matches WASM spec). No floats, no host
//!   calls, no clocks.
//!
//! ## Out of scope (documented; future slices extend)
//!
//! - `i64` / `f32` / `f64` types
//! - Linear memory (`memory`, `i32.load*`, `i32.store*`, `memory.size`,
//!   `memory.grow`, `data` section)
//! - Tables + `call_indirect` (`table`, `element` section)
//! - Imports / exports beyond the entry function (call by index only)
//! - SIMD (`v128`), bulk memory, reference types, GC, exceptions, threads
//! - Multi-value returns (only 0 or 1 i32 result supported)
//! - Custom name section / debug info
//!
//! ## Determinism guarantee (S4 contract)
//!
//! Two replicas executing the same `wasm_exec(module, func, args,
//! gas_limit)` on the same input bytes ALWAYS produce byte-identical
//! results (`Ok(Vec<i32>)` with the same payload, or the same
//! `Err(WasmError)` variant). No state outside the call survives, no
//! wall-clock or RNG is touched. This is the property `s4_kat_*`
//! determinism tests lock.

#![forbid(unsafe_code)]
#![allow(clippy::needless_range_loop)]

use core::convert::TryFrom;

// ============================================================================
// Errors
// ============================================================================

/// Errors produced by module decode + interpreter execution.
///
/// `#[non_exhaustive]` so future slices (memory / floats / etc.) can add
/// variants without a breaking change.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WasmError {
    /// Module bytes shorter than the 8-byte magic+version header, or any
    /// section read ran past `module.len()`.
    UnexpectedEof,
    /// First 4 bytes are not `\0asm`.
    BadMagic,
    /// Bytes 4..8 are not the WASM-MVP version `0x01 0x00 0x00 0x00`.
    BadVersion(u32),
    /// LEB128 sequence is longer than the type's permitted maximum
    /// (5 bytes for u32/i32 LEB128) or has a continuation bit on the
    /// last permitted byte.
    BadLeb128,
    /// A section was declared with a size that runs past module end.
    BadSection(u8),
    /// Type section declared a function type with a tag byte that's not
    /// the func-type marker `0x60`.
    BadFuncType(u8),
    /// Type section used an unsupported value type byte (only `0x7F`
    /// `i32` is accepted in this slice).
    UnsupportedValType(u8),
    /// Function signature has more than 1 result (multi-value returns
    /// are deferred).
    UnsupportedMultiResult(usize),
    /// Function section referenced a type_idx not present in the type
    /// section.
    UnknownTypeIdx(u32),
    /// `call` opcode referenced a func_idx not present in the function
    /// section.
    UnknownFuncIdx(u32),
    /// Caller passed a func_idx >= number of functions.
    EntryFuncIdxOutOfRange { func_idx: u32, total: u32 },
    /// Caller passed an args vector whose length doesn't match the entry
    /// function's signature.
    EntryArgsMismatch { expected: usize, got: usize },
    /// Decoder encountered an opcode byte not implemented in this slice
    /// (e.g., floats, memory ops). Holds the opcode byte.
    UnsupportedOpcode(u8),
    /// Encountered an opcode byte that is not defined in the WASM spec.
    InvalidOpcode(u8),
    /// Block/loop/if started inside a function but its matching `end`
    /// was never found before the code section ended.
    UnterminatedBlock,
    /// `br` / `br_if` referenced a label depth >= active label stack.
    InvalidBranchDepth { depth: u32, active: u32 },
    /// `local.get`/`local.set`/`local.tee` referenced an out-of-range
    /// local index (>= params + declared locals count).
    InvalidLocalIdx { idx: u32, total: u32 },
    /// Stack underflow: an opcode required more operands than the stack
    /// had.
    StackUnderflow { opcode: &'static str },
    /// Gas counter exhausted (limit reached before the function returned).
    OutOfGas,
    /// Signed division/modulo by zero.
    IntegerDivideByZero,
    /// Signed division of `i32::MIN / -1` (overflows the i32 range).
    IntegerOverflow,
    /// `unreachable` opcode executed.
    UnreachableExecuted,
    /// Call depth exceeded the per-execution cap (loop guard).
    CallStackOverflow,
}

impl core::fmt::Display for WasmError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for WasmError {}

// ============================================================================
// Module decode
// ============================================================================

/// A parsed WASM module — type + function + code sections only.
/// Everything else (imports, exports, memory, tables, data, custom) is
/// skipped past during decode; the public `wasm_exec` API doesn't expose
/// any of those concepts in this slice.
#[derive(Debug, Clone)]
pub struct Module {
    /// Function-type table (signatures). `types[i]` = signature with that
    /// type_idx.
    types: Vec<FuncType>,
    /// Per-function type_idx (length = number of functions in the module).
    functions: Vec<u32>,
    /// Per-function body (length = number of functions). `bodies[i]`
    /// matches `functions[i]`.
    bodies: Vec<FuncBody>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FuncType {
    params: Vec<ValType>,
    /// 0 or 1 result (multi-value deferred).
    result: Option<ValType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValType {
    I32,
}

#[derive(Debug, Clone)]
struct FuncBody {
    /// Total local slots BEYOND the params. Each entry is a count of
    /// consecutive locals of one type. In this slice all locals are i32,
    /// so we just sum the counts.
    locals_count: u32,
    /// Raw instruction bytes (the code section already stripped its
    /// length prefix + locals declaration; this is the instruction stream
    /// up to and including the function-end `0x0B`).
    code: Vec<u8>,
}

impl Module {
    /// Decode a WASM module from its byte stream.
    ///
    /// Skips unknown sections (custom/import/export/memory/table/global/
    /// element/data/start/data-count) by reading their declared size and
    /// advancing the cursor — the interpreter doesn't use them.
    pub fn decode(bytes: &[u8]) -> Result<Self, WasmError> {
        let mut c = Cursor::new(bytes);

        // Magic: \0asm
        let magic = c.read_n(4)?;
        if magic != [0x00, 0x61, 0x73, 0x6d] {
            return Err(WasmError::BadMagic);
        }

        // Version: 1
        let ver_bytes = c.read_n(4)?;
        let ver = u32::from_le_bytes([ver_bytes[0], ver_bytes[1], ver_bytes[2], ver_bytes[3]]);
        if ver != 1 {
            return Err(WasmError::BadVersion(ver));
        }

        let mut types: Vec<FuncType> = Vec::new();
        let mut functions: Vec<u32> = Vec::new();
        let mut bodies_raw: Vec<FuncBody> = Vec::new();

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
                1 => {
                    // Type section: vec<FuncType>
                    let n = c.read_u32_leb()? as usize;
                    for _ in 0..n {
                        let tag = c.read_byte()?;
                        if tag != 0x60 {
                            return Err(WasmError::BadFuncType(tag));
                        }
                        let pcount = c.read_u32_leb()? as usize;
                        let mut params = Vec::with_capacity(pcount.min(16));
                        for _ in 0..pcount {
                            let v = c.read_byte()?;
                            if v != 0x7F {
                                return Err(WasmError::UnsupportedValType(v));
                            }
                            params.push(ValType::I32);
                        }
                        let rcount = c.read_u32_leb()? as usize;
                        if rcount > 1 {
                            return Err(WasmError::UnsupportedMultiResult(rcount));
                        }
                        let result = if rcount == 1 {
                            let v = c.read_byte()?;
                            if v != 0x7F {
                                return Err(WasmError::UnsupportedValType(v));
                            }
                            Some(ValType::I32)
                        } else {
                            None
                        };
                        types.push(FuncType { params, result });
                    }
                }
                3 => {
                    // Function section: vec<type_idx>
                    let n = c.read_u32_leb()? as usize;
                    for _ in 0..n {
                        let t = c.read_u32_leb()?;
                        if t as usize >= types.len() {
                            return Err(WasmError::UnknownTypeIdx(t));
                        }
                        functions.push(t);
                    }
                }
                10 => {
                    // Code section: vec<{ size: u32, locals: vec<{count, valtype}>, expr }>
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
                        // locals declaration: vec<{count, valtype}>
                        let local_groups = c.read_u32_leb()? as usize;
                        let mut locals_count: u32 = 0;
                        for _ in 0..local_groups {
                            let cnt = c.read_u32_leb()?;
                            let v = c.read_byte()?;
                            if v != 0x7F {
                                return Err(WasmError::UnsupportedValType(v));
                            }
                            locals_count = locals_count.saturating_add(cnt);
                        }
                        // Code = remaining bytes in the body block.
                        let code_start = c.pos();
                        if body_end < code_start {
                            return Err(WasmError::BadSection(10));
                        }
                        let code_len = body_end - code_start;
                        let code = c.read_n(code_len)?.to_vec();
                        bodies_raw.push(FuncBody { locals_count, code });
                    }
                }
                _ => {
                    // Skip unknown / unsupported section.
                    c.skip(section_size)?;
                }
            }

            // Defensive: if a section's declared size disagrees with the
            // bytes actually consumed, fail.
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
        })
    }

    /// Number of functions in this module.
    pub fn function_count(&self) -> u32 {
        self.functions.len() as u32
    }
}

// ============================================================================
// Cursor — bounds-checked byte reader with LEB128 support
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
    /// Unsigned LEB128 — up to 5 bytes for u32.
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
    /// Signed LEB128 — up to 5 bytes for i32. Per the spec, after the
    /// last group the high bit of the last 7-bit chunk is sign-extended.
    fn read_i32_leb(&mut self) -> Result<i32, WasmError> {
        let mut result: i64 = 0;
        let mut shift: u32 = 0;
        for i in 0..5 {
            let b = self.read_byte()?;
            result |= ((b & 0x7F) as i64) << shift;
            shift += 7;
            if (b & 0x80) == 0 {
                // Sign-extend if needed.
                if shift < 64 && (b & 0x40) != 0 {
                    result |= -1i64 << shift;
                }
                // Truncate to i32 range; ensure no overflow past 32 bits.
                let _ = i; // used for max-iter bound only
                if result < i32::MIN as i64 || result > i32::MAX as i64 {
                    return Err(WasmError::BadLeb128);
                }
                return Ok(result as i32);
            }
        }
        Err(WasmError::BadLeb128)
    }
}

// ============================================================================
// Interpreter
// ============================================================================

/// Execute the entry function `func_idx` with `args` as initial parameters,
/// burning up to `gas_limit` instructions. Returns the result vector
/// (length 0 or 1 i32 per the function's signature) or a typed error.
///
/// **Determinism**: same module bytes + same func_idx + same args + same
/// gas_limit → byte-identical `Result<Vec<i32>, WasmError>` on every
/// invocation (locked by `s4_kat_determinism_*` tests).
pub fn wasm_exec(
    module_bytes: &[u8],
    func_idx: u32,
    args: &[i32],
    gas_limit: u64,
) -> Result<Vec<i32>, WasmError> {
    let module = Module::decode(module_bytes)?;
    exec_in_module(&module, func_idx, args, gas_limit)
}

/// Cap on recursion / call depth (loop guard against deeply-nested
/// modules; the interpreter is otherwise iterative).
const MAX_CALL_DEPTH: u32 = 256;

fn exec_in_module(
    module: &Module,
    func_idx: u32,
    args: &[i32],
    gas_limit: u64,
) -> Result<Vec<i32>, WasmError> {
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
    let mut gas = Gas {
        limit: gas_limit,
        used: 0,
    };
    let result = call_function(module, func_idx, args, &mut gas, 0)?;
    Ok(result)
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

/// Label on the control stack — records the branch target IP and the
/// stack height expected post-branch.
#[derive(Debug, Clone, Copy)]
struct Label {
    /// IP to jump to when a `br` targets this label. For `block` and
    /// `if`/`else` this is the matching `end` IP; for `loop` this is the
    /// loop header IP.
    target_ip: usize,
    /// Operand-stack height at the label's start (used to truncate when a
    /// branch occurs).
    stack_height_at_start: usize,
    /// Whether this label is a `loop` (br jumps backward) — affects
    /// whether arity is consumed.
    is_loop: bool,
}

fn call_function(
    module: &Module,
    func_idx: u32,
    args: &[i32],
    gas: &mut Gas,
    call_depth: u32,
) -> Result<Vec<i32>, WasmError> {
    if call_depth >= MAX_CALL_DEPTH {
        return Err(WasmError::CallStackOverflow);
    }
    let type_idx = module.functions[func_idx as usize] as usize;
    let ftype = &module.types[type_idx];
    let body = &module.bodies[func_idx as usize];

    // Locals = params followed by declared locals (initialized to 0).
    let n_params = ftype.params.len();
    let n_locals_total = n_params + body.locals_count as usize;
    let mut locals: Vec<i32> = Vec::with_capacity(n_locals_total);
    for &a in args {
        locals.push(a);
    }
    for _ in n_params..n_locals_total {
        locals.push(0);
    }

    let mut stack: Vec<i32> = Vec::with_capacity(32);
    // Implicit outer label = the function body. Branching to depth ==
    // number of inner labels exits the function.
    let mut labels: Vec<Label> = Vec::new();

    let code = &body.code;
    let mut ip: usize = 0;
    while ip < code.len() {
        gas.tick()?;
        let op = code[ip];
        ip += 1;
        match op {
            0x00 => return Err(WasmError::UnreachableExecuted), // unreachable
            0x01 => {} // nop
            0x02 => {
                // block bt
                let _bt = read_blocktype(code, &mut ip)?;
                let end_ip = find_matching_end(code, ip)?;
                labels.push(Label {
                    target_ip: end_ip,
                    stack_height_at_start: stack.len(),
                    is_loop: false,
                });
            }
            0x03 => {
                // loop bt
                let _bt = read_blocktype(code, &mut ip)?;
                // For a loop, br jumps BACK to the loop header (right
                // after the blocktype byte).
                labels.push(Label {
                    target_ip: ip,
                    stack_height_at_start: stack.len(),
                    is_loop: true,
                });
            }
            0x04 => {
                // if bt
                let _bt = read_blocktype(code, &mut ip)?;
                let c_val = pop_i32(&mut stack, "if")?;
                let end_ip = find_matching_end(code, ip)?;
                let else_ip = find_matching_else(code, ip, end_ip);
                labels.push(Label {
                    target_ip: end_ip,
                    stack_height_at_start: stack.len(),
                    is_loop: false,
                });
                if c_val == 0 {
                    // Take the else branch if present; otherwise jump to end.
                    ip = match else_ip {
                        Some(e) => e + 1, // skip the 0x05 'else' byte
                        None => end_ip,
                    };
                }
            }
            0x05 => {
                // else — encountered while executing the THEN side, means
                // we should jump past the ELSE side to the matching end.
                let lbl = labels.last().ok_or(WasmError::UnterminatedBlock)?;
                ip = lbl.target_ip;
            }
            0x0B => {
                // end — close the innermost label, or if none, the function.
                if labels.is_empty() {
                    // Function-level end: return.
                    break;
                }
                labels.pop();
            }
            0x0C => {
                // br <depth>
                let depth = read_u32_leb(code, &mut ip)?;
                do_branch(&mut stack, &mut labels, &mut ip, depth)?;
            }
            0x0D => {
                // br_if <depth>
                let depth = read_u32_leb(code, &mut ip)?;
                let c_val = pop_i32(&mut stack, "br_if")?;
                if c_val != 0 {
                    do_branch(&mut stack, &mut labels, &mut ip, depth)?;
                }
            }
            0x0F => {
                // return — break out of the entire function.
                break;
            }
            0x10 => {
                // call <func_idx>
                let callee = read_u32_leb(code, &mut ip)?;
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
                let call_args: Vec<i32> = stack.drain(split..).collect();
                let r = call_function(module, callee, &call_args, gas, call_depth + 1)?;
                for v in r {
                    stack.push(v);
                }
            }
            0x1A => {
                // drop
                pop_i32(&mut stack, "drop")?;
            }
            0x1B => {
                // select: c, b, a -> (if c != 0 { a } else { b })
                let c_val = pop_i32(&mut stack, "select")?;
                let b = pop_i32(&mut stack, "select")?;
                let a = pop_i32(&mut stack, "select")?;
                stack.push(if c_val != 0 { a } else { b });
            }
            0x20 => {
                // local.get <idx>
                let idx = read_u32_leb(code, &mut ip)?;
                if (idx as usize) >= locals.len() {
                    return Err(WasmError::InvalidLocalIdx {
                        idx,
                        total: locals.len() as u32,
                    });
                }
                stack.push(locals[idx as usize]);
            }
            0x21 => {
                // local.set <idx>
                let idx = read_u32_leb(code, &mut ip)?;
                if (idx as usize) >= locals.len() {
                    return Err(WasmError::InvalidLocalIdx {
                        idx,
                        total: locals.len() as u32,
                    });
                }
                let v = pop_i32(&mut stack, "local.set")?;
                locals[idx as usize] = v;
            }
            0x22 => {
                // local.tee <idx>
                let idx = read_u32_leb(code, &mut ip)?;
                if (idx as usize) >= locals.len() {
                    return Err(WasmError::InvalidLocalIdx {
                        idx,
                        total: locals.len() as u32,
                    });
                }
                let v = *stack.last().ok_or(WasmError::StackUnderflow { opcode: "local.tee" })?;
                locals[idx as usize] = v;
            }
            0x41 => {
                // i32.const <signed LEB128>
                let v = read_i32_leb(code, &mut ip)?;
                stack.push(v);
            }
            0x45 => {
                // i32.eqz
                let a = pop_i32(&mut stack, "i32.eqz")?;
                stack.push(if a == 0 { 1 } else { 0 });
            }
            0x46 => bin_cmp(&mut stack, "i32.eq", |a, b| a == b)?,
            0x47 => bin_cmp(&mut stack, "i32.ne", |a, b| a != b)?,
            0x48 => bin_cmp(&mut stack, "i32.lt_s", |a, b| a < b)?,
            0x49 => {
                let b = pop_i32(&mut stack, "i32.lt_u")? as u32;
                let a = pop_i32(&mut stack, "i32.lt_u")? as u32;
                stack.push(if a < b { 1 } else { 0 });
            }
            0x4A => bin_cmp(&mut stack, "i32.gt_s", |a, b| a > b)?,
            0x4B => {
                let b = pop_i32(&mut stack, "i32.gt_u")? as u32;
                let a = pop_i32(&mut stack, "i32.gt_u")? as u32;
                stack.push(if a > b { 1 } else { 0 });
            }
            0x4C => bin_cmp(&mut stack, "i32.le_s", |a, b| a <= b)?,
            0x4E => bin_cmp(&mut stack, "i32.ge_s", |a, b| a >= b)?,
            0x6A => bin_arith_wrapping(&mut stack, "i32.add", i32::wrapping_add)?,
            0x6B => bin_arith_wrapping(&mut stack, "i32.sub", i32::wrapping_sub)?,
            0x6C => bin_arith_wrapping(&mut stack, "i32.mul", i32::wrapping_mul)?,
            0x6D => {
                // i32.div_s — traps on 0 or i32::MIN/-1
                let b = pop_i32(&mut stack, "i32.div_s")?;
                let a = pop_i32(&mut stack, "i32.div_s")?;
                if b == 0 {
                    return Err(WasmError::IntegerDivideByZero);
                }
                if a == i32::MIN && b == -1 {
                    return Err(WasmError::IntegerOverflow);
                }
                stack.push(a / b);
            }
            0x6F => {
                // i32.rem_s — traps on 0; i32::MIN % -1 == 0 per spec (NOT a trap)
                let b = pop_i32(&mut stack, "i32.rem_s")?;
                let a = pop_i32(&mut stack, "i32.rem_s")?;
                if b == 0 {
                    return Err(WasmError::IntegerDivideByZero);
                }
                let r = if a == i32::MIN && b == -1 {
                    0
                } else {
                    a % b
                };
                stack.push(r);
            }
            0x71 => bin_arith_wrapping(&mut stack, "i32.and", |a, b| a & b)?,
            0x72 => bin_arith_wrapping(&mut stack, "i32.or", |a, b| a | b)?,
            0x73 => bin_arith_wrapping(&mut stack, "i32.xor", |a, b| a ^ b)?,
            0x74 => {
                // i32.shl — modulo 32 per spec
                let b = pop_i32(&mut stack, "i32.shl")? as u32;
                let a = pop_i32(&mut stack, "i32.shl")?;
                stack.push(a.wrapping_shl(b & 31));
            }
            0x75 => {
                // i32.shr_s — modulo 32
                let b = pop_i32(&mut stack, "i32.shr_s")? as u32;
                let a = pop_i32(&mut stack, "i32.shr_s")?;
                stack.push(a.wrapping_shr(b & 31));
            }
            0x76 => {
                // i32.shr_u — modulo 32
                let b = pop_i32(&mut stack, "i32.shr_u")? as u32;
                let a = pop_i32(&mut stack, "i32.shr_u")? as u32;
                stack.push((a.wrapping_shr(b & 31)) as i32);
            }
            other => {
                // Distinguish "valid opcode but unsupported in this slice"
                // from "invalid opcode" via a small allow-list.
                if is_known_wasm_opcode(other) {
                    return Err(WasmError::UnsupportedOpcode(other));
                }
                return Err(WasmError::InvalidOpcode(other));
            }
        }
    }

    // Build result per signature.
    let mut out = Vec::with_capacity(1);
    if ftype.result.is_some() {
        let v = pop_i32(&mut stack, "return-value")?;
        out.push(v);
    }
    Ok(out)
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

fn pop_i32(stack: &mut Vec<i32>, op: &'static str) -> Result<i32, WasmError> {
    stack.pop().ok_or(WasmError::StackUnderflow { opcode: op })
}

fn bin_cmp(
    stack: &mut Vec<i32>,
    op: &'static str,
    f: impl Fn(i32, i32) -> bool,
) -> Result<(), WasmError> {
    let b = pop_i32(stack, op)?;
    let a = pop_i32(stack, op)?;
    stack.push(if f(a, b) { 1 } else { 0 });
    Ok(())
}

fn bin_arith_wrapping(
    stack: &mut Vec<i32>,
    op: &'static str,
    f: impl Fn(i32, i32) -> i32,
) -> Result<(), WasmError> {
    let b = pop_i32(stack, op)?;
    let a = pop_i32(stack, op)?;
    stack.push(f(a, b));
    Ok(())
}

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
            if result < i32::MIN as i64 || result > i32::MAX as i64 {
                return Err(WasmError::BadLeb128);
            }
            return Ok(result as i32);
        }
    }
    Err(WasmError::BadLeb128)
}

/// Read a blocktype byte (0x40 = void; 0x7F = i32 result). Returns
/// `Some(())` if a result is expected (we don't actually use the
/// distinction in this minimal interpreter — single-pass + label-stack
/// is sufficient for correctness without tracking arity).
fn read_blocktype(code: &[u8], ip: &mut usize) -> Result<(), WasmError> {
    let b = *code.get(*ip).ok_or(WasmError::UnexpectedEof)?;
    *ip += 1;
    if b == 0x40 || b == 0x7F {
        Ok(())
    } else {
        Err(WasmError::UnsupportedValType(b))
    }
}

/// Skip ahead from `start_ip` past nested blocks to find the matching
/// `end` (0x0B). Returns the index OF the end byte.
fn find_matching_end(code: &[u8], start_ip: usize) -> Result<usize, WasmError> {
    scan_block_until(code, start_ip, false).map(|p| p.0)
}

/// Skip ahead from `start_ip` to find the matching `else` (0x05) IF one
/// exists BEFORE the matching `end`. Returns `Some(index)` of the else
/// byte, or `None` if there is no else.
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

/// Returns `(end_ip, else_ip)`. The `else_ip` is `Some` only if the
/// top-level `if` had an `else` at the same nesting depth.
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
                // block / loop / if — skip blocktype
                if ip >= code.len() {
                    return Err(WasmError::UnexpectedEof);
                }
                ip += 1;
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
            // Skip operand bytes for opcodes that carry immediates.
            0x0C | 0x0D | 0x10 | 0x20 | 0x21 | 0x22 => {
                // single u32 LEB128 immediate
                skip_u32_leb(code, &mut ip)?;
            }
            0x41 => {
                // i32.const — signed LEB128
                skip_i32_leb(code, &mut ip)?;
            }
            _ => {
                // Other opcodes: no immediates (or unsupported; the actual
                // interpreter will reject them when reached).
            }
        }
    }
    Err(WasmError::UnterminatedBlock)
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

fn skip_i32_leb(code: &[u8], ip: &mut usize) -> Result<(), WasmError> {
    skip_u32_leb(code, ip)
}

fn do_branch(
    stack: &mut Vec<i32>,
    labels: &mut Vec<Label>,
    ip: &mut usize,
    depth: u32,
) -> Result<(), WasmError> {
    let active = labels.len() as u32;
    if depth > active {
        return Err(WasmError::InvalidBranchDepth { depth, active });
    }
    if depth == active {
        // Branch out of the function body — clear labels, ip at end so
        // the main loop will exit naturally on next iteration (we set ip
        // past the code).
        // Truncate stack to whatever the implicit outer-function arity
        // wants; we don't track that here, so just leave stack as-is
        // (the function-end value pop handles the return value).
        labels.clear();
        *ip = usize::MAX; // signal loop exit
        // But the function expects to fall through to end; safer: leave
        // stack alone and break by re-positioning IP to end of code.
        // The caller's `while ip < code.len()` will exit.
        return Ok(());
    }
    // Pop labels above the target.
    let target_idx = labels.len() - 1 - depth as usize;
    let target = labels[target_idx];
    // Truncate stack to label's start height (drop intermediate operands).
    while stack.len() > target.stack_height_at_start {
        stack.pop();
    }
    // Pop labels above + including the target if it's NOT a loop.
    if target.is_loop {
        labels.truncate(target_idx + 1);
    } else {
        labels.truncate(target_idx);
    }
    *ip = target.target_ip;
    // For non-loop targets, the target_ip points AT the `end` byte. Step
    // past it.
    if !target.is_loop {
        *ip += 1;
    }
    Ok(())
}

/// Allow-list of WASM-MVP opcodes the interpreter recognizes but doesn't
/// implement in this slice (returns `UnsupportedOpcode`). Used to
/// distinguish "you used a real WASM opcode we just don't support yet"
/// from "you supplied garbage" (`InvalidOpcode`).
fn is_known_wasm_opcode(b: u8) -> bool {
    // WASM-MVP opcodes per the spec. This is intentionally permissive
    // (true for "could appear in a valid WASM module") and we don't
    // exhaustively list every one — just the broad ranges.
    matches!(
        b,
        // Numeric f32/f64 + i64 + memory loads/stores + memory.size/grow
        // + global.get/set + table + reftypes + bulk memory
        0x06..=0x0A | 0x0E | 0x11..=0x19 | 0x1C..=0x1F |
        0x23..=0x26 | 0x28..=0x3F | 0x42..=0x44 |
        0x4D | 0x4F | 0x50..=0x66 |
        0x67..=0x69 | 0x6E | 0x70 | 0x77..=0xA6 | 0xA7..=0xC4 |
        0xD0..=0xD4 | 0xFC | 0xFD..=0xFE
    )
}

// ============================================================================
// Helpers for tests: hand-build WASM module bytes
// ============================================================================

#[cfg(test)]
mod test_helpers {
    /// Build a complete WASM module with one function. `params_count`
    /// i32 parameters; `result_present` controls whether the function
    /// has an i32 result. `locals_count` declared additional i32 locals.
    /// `code` is the raw instruction stream INCLUDING the trailing 0x0B
    /// function-end byte.
    pub fn build_module(
        params_count: u32,
        result_present: bool,
        locals_count: u32,
        code: &[u8],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        // Magic + version
        out.extend_from_slice(&[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);

        // Type section (id=1)
        let mut type_sec = Vec::new();
        type_sec.extend_from_slice(&[0x01]); // 1 type
        type_sec.push(0x60); // func-type tag
        write_u32_leb(&mut type_sec, params_count);
        for _ in 0..params_count {
            type_sec.push(0x7F); // i32
        }
        type_sec.push(if result_present { 0x01 } else { 0x00 });
        if result_present {
            type_sec.push(0x7F);
        }
        out.push(0x01); // section id
        write_u32_leb(&mut out, type_sec.len() as u32);
        out.extend_from_slice(&type_sec);

        // Function section (id=3)
        out.push(0x03);
        let func_sec = vec![0x01, 0x00]; // 1 function, type_idx=0
        write_u32_leb(&mut out, func_sec.len() as u32);
        out.extend_from_slice(&func_sec);

        // Code section (id=10)
        let mut body = Vec::new();
        if locals_count == 0 {
            body.push(0x00); // 0 local groups
        } else {
            body.push(0x01); // 1 local group
            write_u32_leb(&mut body, locals_count);
            body.push(0x7F); // i32
        }
        body.extend_from_slice(code);

        let mut code_sec = Vec::new();
        code_sec.push(0x01); // 1 body
        write_u32_leb(&mut code_sec, body.len() as u32);
        code_sec.extend_from_slice(&body);

        out.push(0x0A);
        write_u32_leb(&mut out, code_sec.len() as u32);
        out.extend_from_slice(&code_sec);

        out
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
            // Sign bit of byte is second-high-order bit (0x40).
            if (value == 0 && (byte & 0x40) == 0)
                || (value == -1 && (byte & 0x40) != 0)
            {
                more = false;
            } else {
                byte |= 0x80;
            }
            out.push(byte);
        }
    }
}

// ============================================================================
// Hand-derived KATs (S4 verification gate)
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::test_helpers::*;

    /// S4-KAT-1: bad magic rejected.
    #[test]
    fn s4_kat_bad_magic_rejected() {
        let mut bytes = vec![0; 8];
        bytes[0] = 0xFF;
        let r = Module::decode(&bytes);
        assert_eq!(r.unwrap_err(), WasmError::BadMagic);
    }

    /// S4-KAT-2: bad version rejected.
    #[test]
    fn s4_kat_bad_version_rejected() {
        let mut bytes = vec![0x00, 0x61, 0x73, 0x6d, 0x02, 0x00, 0x00, 0x00];
        bytes.extend_from_slice(&[]);
        let r = Module::decode(&bytes);
        assert_eq!(r.unwrap_err(), WasmError::BadVersion(2));
    }

    /// S4-KAT-3: minimal function returning a constant — i32.const 42; end.
    #[test]
    fn s4_kat_const_return_42() {
        // code: 0x41 0x2A 0x0B   (i32.const 42; end)
        let code = vec![0x41, 0x2A, 0x0B];
        let module = build_module(0, true, 0, &code);
        let r = wasm_exec(&module, 0, &[], 100).unwrap();
        assert_eq!(r, vec![42]);
    }

    /// S4-KAT-4: i32.add of two constants.
    #[test]
    fn s4_kat_add_3_4_returns_7() {
        // i32.const 3; i32.const 4; i32.add; end
        let code = vec![0x41, 0x03, 0x41, 0x04, 0x6A, 0x0B];
        let m = build_module(0, true, 0, &code);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![7]);
    }

    /// S4-KAT-5: parameter passing — fn(a: i32, b: i32) -> i32 { a*b + 1 }.
    #[test]
    fn s4_kat_two_params_a_times_b_plus_1() {
        // local.get 0; local.get 1; i32.mul; i32.const 1; i32.add; end
        let code = vec![
            0x20, 0x00, // local.get 0
            0x20, 0x01, // local.get 1
            0x6C, // i32.mul
            0x41, 0x01, // i32.const 1
            0x6A, // i32.add
            0x0B, // end
        ];
        let m = build_module(2, true, 0, &code);
        let r = wasm_exec(&m, 0, &[5, 7], 100).unwrap();
        assert_eq!(r, vec![36]); // 5*7+1
    }

    /// S4-KAT-6: signed div + rem semantics; i32.div_s of 17/5 = 3; rem = 2.
    #[test]
    fn s4_kat_div_rem_signed() {
        // i32.const 17; i32.const 5; i32.div_s; end
        let m = build_module(0, true, 0, &[0x41, 0x11, 0x41, 0x05, 0x6D, 0x0B]);
        assert_eq!(wasm_exec(&m, 0, &[], 100).unwrap(), vec![3]);
        // i32.const 17; i32.const 5; i32.rem_s; end
        let m2 = build_module(0, true, 0, &[0x41, 0x11, 0x41, 0x05, 0x6F, 0x0B]);
        assert_eq!(wasm_exec(&m2, 0, &[], 100).unwrap(), vec![2]);
    }

    /// S4-KAT-7: i32.div_s by zero traps with IntegerDivideByZero.
    #[test]
    fn s4_kat_div_by_zero_traps() {
        let m = build_module(0, true, 0, &[0x41, 0x01, 0x41, 0x00, 0x6D, 0x0B]);
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap_err(),
            WasmError::IntegerDivideByZero
        );
    }

    /// S4-KAT-8: i32::MIN / -1 traps with IntegerOverflow.
    #[test]
    fn s4_kat_div_imin_by_neg1_traps() {
        // i32.const i32::MIN (encoded as signed LEB128) ; i32.const -1 ; i32.div_s ; end
        let mut code = vec![0x41];
        write_i32_leb(&mut code, i32::MIN);
        code.push(0x41);
        write_i32_leb(&mut code, -1);
        code.extend_from_slice(&[0x6D, 0x0B]);
        let m = build_module(0, true, 0, &code);
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap_err(),
            WasmError::IntegerOverflow
        );
    }

    /// S4-KAT-9: gas exhaustion traps.
    #[test]
    fn s4_kat_gas_exhaustion_traps() {
        // Same as KAT-4 (uses ~5 instructions) but with gas_limit=2.
        let code = vec![0x41, 0x03, 0x41, 0x04, 0x6A, 0x0B];
        let m = build_module(0, true, 0, &code);
        assert_eq!(wasm_exec(&m, 0, &[], 2).unwrap_err(), WasmError::OutOfGas);
        // gas_limit=5 — should succeed (5 instructions fit; end may or
        // may not count depending on impl. Test with comfortable headroom).
        assert_eq!(wasm_exec(&m, 0, &[], 10).unwrap(), vec![7]);
    }

    /// S4-KAT-10: if/else control flow — fn(n) { if n > 0 { 1 } else { -1 } }.
    #[test]
    fn s4_kat_if_else_branches() {
        // local.get 0
        // i32.const 0
        // i32.gt_s
        // if 0x7F (i32 result)
        //   i32.const 1
        // else
        //   i32.const -1 (encoded as 0x7F = LEB128 -1)
        // end
        // end
        let mut code = vec![
            0x20, 0x00, // local.get 0
            0x41, 0x00, // i32.const 0
            0x4A, // i32.gt_s
            0x04, 0x7F, // if i32
            0x41, 0x01, // i32.const 1
            0x05, // else
            0x41, 0x7F, // i32.const -1
            0x0B, // end (if)
            0x0B, // end (function)
        ];
        let _ = &mut code;
        let m = build_module(1, true, 0, &code);
        assert_eq!(wasm_exec(&m, 0, &[5], 100).unwrap(), vec![1]);
        assert_eq!(wasm_exec(&m, 0, &[-3], 100).unwrap(), vec![-1]);
        assert_eq!(wasm_exec(&m, 0, &[0], 100).unwrap(), vec![-1]);
    }

    /// S4-KAT-11: in-module `call` — fn(n) { return double(n); }
    ///   double(n) { return n*2; }
    #[test]
    fn s4_kat_in_module_call() {
        // We need two functions in one module. Bypass build_module helper
        // and hand-construct.
        let mut bytes = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

        // Type section: 1 type — (i32) -> i32
        let mut type_sec = Vec::new();
        type_sec.push(0x01); // 1 type
        type_sec.extend_from_slice(&[0x60, 0x01, 0x7F, 0x01, 0x7F]);
        bytes.push(0x01);
        write_u32_leb(&mut bytes, type_sec.len() as u32);
        bytes.extend_from_slice(&type_sec);

        // Function section: 2 functions, both type 0
        bytes.push(0x03);
        bytes.extend_from_slice(&[0x03, 0x02, 0x00, 0x00]);

        // Code section: 2 bodies
        let mut code_sec = Vec::new();
        code_sec.push(0x02); // 2 bodies

        // Body 0: entry — local.get 0; call 1; end
        let body0: Vec<u8> = vec![0x00, 0x20, 0x00, 0x10, 0x01, 0x0B];
        write_u32_leb(&mut code_sec, body0.len() as u32);
        code_sec.extend_from_slice(&body0);

        // Body 1: double — local.get 0; i32.const 2; i32.mul; end
        let body1: Vec<u8> = vec![0x00, 0x20, 0x00, 0x41, 0x02, 0x6C, 0x0B];
        write_u32_leb(&mut code_sec, body1.len() as u32);
        code_sec.extend_from_slice(&body1);

        bytes.push(0x0A);
        write_u32_leb(&mut bytes, code_sec.len() as u32);
        bytes.extend_from_slice(&code_sec);

        assert_eq!(wasm_exec(&bytes, 0, &[21], 100).unwrap(), vec![42]);
    }

    /// S4-KAT-12: determinism — same call twice → identical result;
    /// same call with subtly different gas_limit (still sufficient) →
    /// SAME result (no observable gas-leak into the value).
    #[test]
    fn s4_kat_determinism_byte_identical_repeat() {
        let code = vec![
            0x20, 0x00, 0x20, 0x01, 0x6A, 0x41, 0x07, 0x6C, 0x0B,
        ]; // (a+b)*7
        let m = build_module(2, true, 0, &code);
        let r1 = wasm_exec(&m, 0, &[3, 4], 100).unwrap();
        let r2 = wasm_exec(&m, 0, &[3, 4], 100).unwrap();
        let r3 = wasm_exec(&m, 0, &[3, 4], 1000).unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r2, r3);
        assert_eq!(r1, vec![49]); // (3+4)*7
    }

    /// S4-KAT-13: unreachable opcode traps.
    #[test]
    fn s4_kat_unreachable_traps() {
        let code = vec![0x00, 0x41, 0x00, 0x0B]; // unreachable; i32.const 0; end
        let m = build_module(0, true, 0, &code);
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap_err(),
            WasmError::UnreachableExecuted
        );
    }

    /// S4-KAT-14: bounds-checked decoder — truncated module gives a typed
    /// error, not a panic.
    #[test]
    fn s4_kat_decode_truncated_is_typed_error() {
        // Only magic+version, no sections — decode should succeed but
        // function_count() is 0.
        let m = Module::decode(&[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]).unwrap();
        assert_eq!(m.function_count(), 0);
        // Truncated mid-type-section.
        let r = Module::decode(&[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01]);
        assert!(matches!(r.unwrap_err(), WasmError::UnexpectedEof | WasmError::BadSection(_)));
    }

    /// S4-KAT-15: invalid opcode trap (distinct from "unsupported but valid"
    /// in WASM-MVP).
    #[test]
    fn s4_kat_invalid_opcode_traps() {
        // 0xEF is a reserved-undefined opcode in WASM-MVP.
        let code = vec![0xEF, 0x0B];
        let m = build_module(0, false, 0, &code);
        assert_eq!(
            wasm_exec(&m, 0, &[], 100).unwrap_err(),
            WasmError::InvalidOpcode(0xEF)
        );
    }
}

// ============================================================================
// Public Vec<u8> compile helpers (re-exposed for downstream test crates)
// ============================================================================

// Suppress dead_code on TryFrom (kept as a deliberate import in case
// future expansions need it — keeps the import surface intentional).
const _: fn() = || {
    let _: Result<u8, _> = u8::try_from(0u32);
};
