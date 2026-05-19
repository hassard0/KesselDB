# Parquet Object Sources (OBJ-2a) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `CREATE EXTERNAL SOURCE … FROM 's3://…'|'az://…' FORMAT PARQUET` materializes rows from a PLAIN-encoded, uncompressed, flat-REQUIRED, V1-data-page Parquet object — flipping the OBJ-1 CREATE-time rejection to supported.

**Architecture:** A new pure-Rust **zero-external-dependency** crate `kessel-parquet` (Thrift-compact metadata decode + PLAIN page decode) decodes only the recipe-mapped leaf columns from the whole captured object. `kessel-fetch`'s existing `object-store` feature pulls it; a new `Format::Parquet` arm in `rows_from_body` maps `PqValue → json::Cell` so the **existing `coerce::to_field_bytes` path is reused byte-for-byte** (no new determinism surface). `do_refresh`/`do_refresh_objstore` map format code `3 → Format::Parquet`. `kessel-sql` accepts `FORMAT PARQUET` only for `s3://`/`az://`. Everything downstream (deterministic ObjectId, atomic Op::Txn, fail-closed) is the unchanged OBJ-1 path.

**Tech Stack:** Rust, in-tree only. **No external dependency anywhere** (Thrift Compact Protocol + Parquet footer/PLAIN decoded by hand, KAT-pinned against the *published Apache Thrift / Apache Parquet spec byte layouts* as the independent authority).

**Spec:** `docs/superpowers/specs/2026-05-19-parquet-object-source-design.md`
**Internal record (write in Task 13):** `docs/superpowers/specs/2026-05-19-kesseldb-subproject101-parquet.md`

**Conventions (every task):** repo `C:\Users\ihass\KesselDB`; commit straight to `main` (single-branch, user-authorized via the standing autonomous mandate `feedback_kesseldb_autonomous_build`); **no** `Co-Authored-By`, **no** signing; match `git log -3 --format='%s'` style. After each task's final commit, `git push`. Bash tool (git-bash); forward-slash paths.

**Determinism gate (run after every kernel-adjacent task — T8/T9/T10/T11/T13):** `cargo test --workspace --release` ⇒ every `test result:` line `0 failed` AND `large_seed_corpus_is_deterministic_and_converges` present + passing. Baseline (Task 0) = **267**. `kessel-parquet` is a workspace member ⇒ its unit tests run under `cargo test --workspace`; the default-build total rises **honestly** (not a zero-delta — Task 13 reconciles README/STATUS to the measured number with the real reason, exactly as SP100 did). The kernel pulls **no new external dependency**; the default `cargo build`/`cargo tree` links no parquet/objstore/rustls (verified). Existing EXT/TLS/OBJ-1 oracles MUST stay green unchanged.

**FIXTURE / KAT DISCIPLINE (critical — same stance as OBJ-1's "use a real KAT or BLOCK"):** A "Parquet" fixture that only the reader-under-test can parse is FORBIDDEN (self-referential). The independent authority is the **published Apache Thrift Compact Protocol spec** and the **published Apache Parquet format spec** byte layouts: `thrift.rs`, `footer.rs`, `plain.rs`, `meta.rs` are each KAT-pinned against **hand-computed byte vectors derived from those public specs** (varint/zigzag/field-delta and PLAIN little-endian and the `PAR1`+`[u32 LE len]` footer framing are fully specified and independently hand-verifiable — these are real KATs, like OBJ-1's AWS signing-key constant). The end-to-end fixture (Task 7) is produced by a real external Parquet writer **if one can be installed** (`pip install pyarrow`), else by an **independent spec-faithful generator** committed alongside the bytes; either way the suite is non-self-referential because every decode primitive is pinned to the public-spec byte KATs. If the public-spec primitive KATs cannot be established for a task, that task reports **BLOCKED** — do not fake.

---

## File Structure

- `Cargo.toml` (root) — add `crates/kessel-parquet` workspace member.
- `crates/kessel-parquet/Cargo.toml` — new crate, **no dependencies**.
- `crates/kessel-parquet/src/lib.rs` — `PqValue`, `PqError`(+Display), `pub fn extract`, support-matrix gate, row assembly.
- `crates/kessel-parquet/src/thrift.rs` — Thrift Compact Protocol reader (varint/zigzag/field-delta; bool/i32/i64/binary/list/struct).
- `crates/kessel-parquet/src/footer.rs` — `PAR1` magic + trailing `[u32 LE meta_len][PAR1]` framing + bounds.
- `crates/kessel-parquet/src/meta.rs` — `FileMetaData` structs (schema/row-group/column-chunk/column-metadata/data-page-header + Type/Encoding/Codec/Repetition/PageType enums) via `thrift.rs`.
- `crates/kessel-parquet/src/plain.rs` — PLAIN page decode per physical type.
- `crates/kessel-parquet/tests/fixtures/` — checked-in real/spec-faithful Parquet file(s) + `README.md`.
- `crates/kessel-fetch/Cargo.toml` — `object-store` feature gains `dep:kessel-parquet`.
- `crates/kessel-fetch/src/lib.rs` — `Format::Parquet` variant; `rows_from_body` Parquet arm; `pq_to_cell`.
- `crates/kessel-fetch/tests/parquet_decode.rs` — feature-gated `rows_from_body(Format::Parquet)` over the fixture.
- `crates/kesseldb-server/src/router.rs` — `3 => Format::Parquet` at both format-match sites (~620, ~998).
- `crates/kessel-sql/src/lib.rs` — flip the OBJ-1 `FORMAT PARQUET` rejection; the 3 new CREATE-time rejections.
- `crates/kesseldb-server/tests/external_source_parquet_oracle.rs` — feature-gated fail-closed e2e.
- `docs/USAGE.md`, `docs/STATUS.md`, `README.md`, the subproject101 record.

---

### Task 0: Record the determinism baseline

**Files:** none.

- [ ] **Step 1: Capture the default-build total + seed-7 + default dep cleanliness**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus" | awk '/large_seed/{print} /ok\./{n+=$4} /[1-9][0-9]* failed/{print "REALFAIL"} END{print "TOTAL="n}'`
Expected: `TOTAL=267`, `large_seed_corpus_is_deterministic_and_converges ... ok`, no `REALFAIL`. Record **BASELINE=267**.
Run: `cd /c/Users/ihass/KesselDB && cargo tree -p kesseldb-server -e normal 2>/dev/null | grep -iE "rustls|webpki|objstore|parquet" || echo "DEFAULT CLEAN"`
Expected: `DEFAULT CLEAN`. No commit.

---

### Task 1: Scaffold `kessel-parquet` (API + workspace wiring)

**Files:**
- Modify: root `Cargo.toml`
- Create: `crates/kessel-parquet/Cargo.toml`, `crates/kessel-parquet/src/lib.rs`, empty `src/{thrift.rs,footer.rs,meta.rs,plain.rs}`

- [ ] **Step 1: Add the crate to the workspace**

Run `cd /c/Users/ihass/KesselDB && grep -n "members" Cargo.toml && sed -n '1,40p' Cargo.toml`. Add `"crates/kessel-parquet"` to the `members` array, matching the exact existing formatting (the same way `"crates/kessel-objstore"` was added in SP100).

- [ ] **Step 2: Create `crates/kessel-parquet/Cargo.toml`**

```toml
[package]
name = "kessel-parquet"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
# Intentionally EMPTY. A minimal Thrift-compact + PLAIN Parquet
# reader is hand-implemented; this crate adds NO external dependency
# anywhere (the deterministic-kernel zero-dep invariant).

[lib]
path = "src/lib.rs"
```

- [ ] **Step 3: Create `crates/kessel-parquet/src/lib.rs`**

```rust
//! Minimal pure-Rust Parquet reader (OBJ-2a): PLAIN encoding,
//! UNCOMPRESSED, flat REQUIRED columns, V1 data pages, multi
//! row-group, recipe-mapped leaf-column subset. Zero external
//! dependencies. Never compiled by the default KesselDB build
//! (only `kessel-fetch`'s `object-store` feature pulls it), so the
//! deterministic kernel + seed-7 corpus are untouched.
#![forbid(unsafe_code)]

mod thrift;
mod footer;
mod meta;
mod plain;

/// One decoded Parquet physical value, format-agnostic. The caller
/// (`kessel-fetch`) maps this to its own `Cell` so the existing
/// coerce path is reused unchanged.
#[derive(Clone, Debug, PartialEq)]
pub enum PqValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Bytes(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PqError {
    /// Malformed / truncated / out-of-bounds Parquet bytes.
    Bad(String),
    /// Well-formed but uses a feature outside OBJ-2a (names the
    /// OBJ-2b/2c follow-on).
    Unsupported(String),
}

impl std::fmt::Display for PqError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PqError::Bad(s) => write!(f, "parquet: {s}"),
            PqError::Unsupported(s) => write!(f, "parquet unsupported: {s}"),
        }
    }
}

/// Decode the `wanted` leaf columns (in that output order) from a
/// whole Parquet object. OBJ-2a: flat REQUIRED columns, PLAIN,
/// UNCOMPRESSED, V1 data pages, all row groups concatenated.
pub fn extract(
    _bytes: &[u8],
    _wanted: &[&str],
) -> Result<Vec<Vec<PqValue>>, PqError> {
    Err(PqError::Bad("extract not implemented (Task 6)".into()))
}
```

- [ ] **Step 4: Create the four module stubs so the crate compiles**

`crates/kessel-parquet/src/thrift.rs`, `footer.rs`, `meta.rs`, `plain.rs` — each EXACTLY:

```rust
//! (filled in a later task)
#![allow(dead_code)] // consumed by lib.rs extract() in Task 6
```

- [ ] **Step 5: Build + isolation check**

Run: `cd /c/Users/ihass/KesselDB && cargo build -p kessel-parquet && cargo test -p kessel-parquet 2>&1 | tail -2`
Expected: builds; `0 passed; 0 failed` (no tests yet).
Run: `cd /c/Users/ihass/KesselDB && cargo tree -p kessel-fetch -e normal | grep -i parquet || echo "PARQUET NOT IN DEFAULT GRAPH"`
Expected: `PARQUET NOT IN DEFAULT GRAPH`.

- [ ] **Step 6: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add Cargo.toml crates/kessel-parquet
git commit -m "parquet: scaffold kessel-parquet crate (PqValue/PqError/extract api)"
```

---

### Task 2: Thrift Compact Protocol reader (`thrift.rs`) — spec-published-byte KAT

The Thrift Compact Protocol is fully specified (varint LEB128, zig-zag, 4-bit field-delta + type-nibble headers). These byte layouts are hand-verifiable from the published spec — a genuine independent KAT.

**Files:** Modify `crates/kessel-parquet/src/thrift.rs`

- [ ] **Step 1: Write the failing KAT test**

Replace `crates/kessel-parquet/src/thrift.rs` with the implementation in Step 3 PLUS this test module. The asserted bytes are hand-computed from the Apache Thrift Compact Protocol spec (varint/zig-zag/struct field-headers); each is annotated with its derivation so a reviewer can re-verify against the public spec independently of this code.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_and_zigzag_spec_kat() {
        // ULEB128 (Thrift compact varint), hand-derived from the spec:
        //   0 -> [0x00]; 1 -> [0x01]; 127 -> [0x7f];
        //   128 -> [0x80,0x01]; 300 -> [0xac,0x02]
        for (n, bytes) in [
            (0u64, &[0x00u8][..]),
            (1, &[0x01]),
            (127, &[0x7f]),
            (128, &[0x80, 0x01]),
            (300, &[0xac, 0x02]),
        ] {
            let mut c = Reader::new(bytes);
            assert_eq!(c.uvarint().unwrap(), n, "uvarint {n}");
            assert!(c.at_end());
        }
        // zig-zag i64 (spec): 0->0, -1->1, 1->2, -2->3, 2->4,
        //   2147483647 -> 4294967294
        for (v, z) in [
            (0i64, 0u64),
            (-1, 1),
            (1, 2),
            (-2, 3),
            (2, 4),
            (2147483647, 4294967294),
        ] {
            assert_eq!(zigzag_decode(z), v, "zigzag {z}->{v}");
        }
    }

    #[test]
    fn struct_field_header_and_types_spec_kat() {
        // Compact struct (spec): field header byte = (delta<<4)|type.
        // type 3 = i32, 5 = i64, 8 = binary, 2 = BOOL_TRUE,
        // 1 = BOOL_TRUE? — use the spec's compact type ids:
        //   1 BOOLEAN_TRUE, 2 BOOLEAN_FALSE, 5 I32, 6 I64, 8 BINARY,
        //   9 LIST, 12 STRUCT. (See impl `CType`.)
        // Build: { field 1: i32 = 7, field 2: i64 = -2, field 3:
        //          binary = "hi", stop } using delta-encoded headers.
        // header f1 i32: delta=1,type=5 -> 0x15 ; value zigzag(7)=14 -> 0x0e
        // header f2 i64: delta=1,type=6 -> 0x16 ; value zigzag(-2)=3 -> 0x03
        // header f3 bin: delta=1,type=8 -> 0x18 ; len=2 -> 0x02, "hi"
        // stop: 0x00
        let bytes = [
            0x15, 0x0e, 0x16, 0x03, 0x18, 0x02, b'h', b'i', 0x00,
        ];
        let mut s = StructReader::new(&bytes);
        let f1 = s.next_field().unwrap().unwrap();
        assert_eq!(f1.id, 1);
        assert_eq!(s.read_i32(&f1).unwrap(), 7);
        let f2 = s.next_field().unwrap().unwrap();
        assert_eq!(f2.id, 2);
        assert_eq!(s.read_i64(&f2).unwrap(), -2);
        let f3 = s.next_field().unwrap().unwrap();
        assert_eq!(f3.id, 3);
        assert_eq!(s.read_binary(&f3).unwrap(), b"hi");
        assert!(s.next_field().unwrap().is_none()); // STOP
    }

    #[test]
    fn truncated_input_is_typed_error_not_panic() {
        let mut c = Reader::new(&[0x80]); // varint continues, no more
        assert!(c.uvarint().is_err());
        let mut s = StructReader::new(&[0x18, 0x05]); // binary len 5, no data
        let f = s.next_field().unwrap().unwrap();
        assert!(s.read_binary(&f).is_err());
    }
}
```

- [ ] **Step 2: Run it — expect failure**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet thrift -- --nocapture`
Expected: FAIL to compile (`Reader`/`StructReader`/`zigzag_decode` undefined).

- [ ] **Step 3: Implement `thrift.rs`**

Put this ABOVE the `#[cfg(test)]`:

```rust
//! Minimal Thrift Compact Protocol reader — only the subset Parquet
//! `FileMetaData` uses. Spec: Apache Thrift "compact protocol".
//! Every read is bounds-checked; malformed input ⇒ `Err`, no panic.
#![allow(dead_code)] // consumed by meta.rs / lib.rs

pub type TResult<T> = Result<T, crate::PqError>;

fn err(s: &str) -> crate::PqError {
    crate::PqError::Bad(s.to_string())
}

/// Compact field types (Thrift compact spec).
pub mod ctype {
    pub const BOOL_TRUE: u8 = 1;
    pub const BOOL_FALSE: u8 = 2;
    pub const I8: u8 = 3;
    pub const I16: u8 = 4;
    pub const I32: u8 = 5;
    pub const I64: u8 = 6;
    pub const DOUBLE: u8 = 7;
    pub const BINARY: u8 = 8;
    pub const LIST: u8 = 9;
    pub const SET: u8 = 10;
    pub const MAP: u8 = 11;
    pub const STRUCT: u8 = 12;
}

pub fn zigzag_decode(u: u64) -> i64 {
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}

pub struct Reader<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> Reader<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Reader { b, p: 0 }
    }
    pub fn at_end(&self) -> bool {
        self.p >= self.b.len()
    }
    pub fn byte(&mut self) -> TResult<u8> {
        let v = *self.b.get(self.p).ok_or_else(|| err("eof: byte"))?;
        self.p += 1;
        Ok(v)
    }
    pub fn uvarint(&mut self) -> TResult<u64> {
        let mut shift = 0u32;
        let mut out = 0u64;
        loop {
            let b = self.byte()?;
            if shift >= 64 {
                return Err(err("varint overflow"));
            }
            out |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(out);
            }
            shift += 7;
        }
    }
    pub fn ivarint(&mut self) -> TResult<i64> {
        Ok(zigzag_decode(self.uvarint()?))
    }
    pub fn take(&mut self, n: usize) -> TResult<&'a [u8]> {
        let end = self.p.checked_add(n).ok_or_else(|| err("len overflow"))?;
        let s = self.b.get(self.p..end).ok_or_else(|| err("eof: take"))?;
        self.p = end;
        Ok(s)
    }
}

#[derive(Clone, Debug)]
pub struct Field {
    pub id: i16,
    pub ctype: u8,
    /// For BOOL_TRUE/BOOL_FALSE the value is in the header itself.
    pub bool_val: Option<bool>,
}

/// Reads one Thrift-compact struct (delta-encoded field headers).
pub struct StructReader<'a> {
    r: Reader<'a>,
    last_id: i16,
}

impl<'a> StructReader<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        StructReader { r: Reader::new(b), last_id: 0 }
    }
    pub fn reader(&mut self) -> &mut Reader<'a> {
        &mut self.r
    }
    /// `Ok(None)` = STOP field (end of struct).
    pub fn next_field(&mut self) -> TResult<Option<Field>> {
        let h = self.r.byte()?;
        if h == 0 {
            return Ok(None);
        }
        let ctype = h & 0x0f;
        let delta = (h >> 4) & 0x0f;
        let id = if delta == 0 {
            // long form: zig-zag i16 follows
            let z = self.r.ivarint()?;
            i16::try_from(z).map_err(|_| err("field id range"))?
        } else {
            self.last_id
                .checked_add(delta as i16)
                .ok_or_else(|| err("field id overflow"))?
        };
        self.last_id = id;
        let bool_val = match ctype {
            ctype::BOOL_TRUE => Some(true),
            ctype::BOOL_FALSE => Some(false),
            _ => None,
        };
        Ok(Some(Field { id, ctype, bool_val }))
    }
    pub fn read_i32(&mut self, f: &Field) -> TResult<i32> {
        if f.ctype != ctype::I32 && f.ctype != ctype::I8
            && f.ctype != ctype::I16
        {
            return Err(err("expected i32"));
        }
        i32::try_from(self.r.ivarint()?).map_err(|_| err("i32 range"))
    }
    pub fn read_i64(&mut self, f: &Field) -> TResult<i64> {
        if f.ctype != ctype::I64 {
            return Err(err("expected i64"));
        }
        self.r.ivarint()
    }
    pub fn read_bool(&mut self, f: &Field) -> TResult<bool> {
        f.bool_val.ok_or_else(|| err("expected bool"))
    }
    pub fn read_binary(&mut self, f: &Field) -> TResult<&'a [u8]> {
        if f.ctype != ctype::BINARY {
            return Err(err("expected binary"));
        }
        let n = usize::try_from(self.r.uvarint()?)
            .map_err(|_| err("binary len range"))?;
        self.r.take(n)
    }
    /// List header: returns (element_ctype, count). Spec: size byte
    /// `(size<<4)|etype`; if size==15 a uvarint count follows.
    pub fn list_header(&mut self) -> TResult<(u8, usize)> {
        let h = self.r.byte()?;
        let etype = h & 0x0f;
        let mut size = (h >> 4) as usize;
        if size == 15 {
            size = usize::try_from(self.r.uvarint()?)
                .map_err(|_| err("list size range"))?;
        }
        Ok((etype, size))
    }
    /// Skip one field's value of the given ctype (for fields we
    /// don't care about). Recursive for struct/list; bounded.
    pub fn skip(&mut self, ctype: u8) -> TResult<()> {
        match ctype {
            ctype::BOOL_TRUE | ctype::BOOL_FALSE => {}
            ctype::I8 | ctype::I16 | ctype::I32 | ctype::I64 => {
                self.r.uvarint()?;
            }
            ctype::DOUBLE => {
                self.r.take(8)?;
            }
            ctype::BINARY => {
                let n = usize::try_from(self.r.uvarint()?)
                    .map_err(|_| err("skip bin len"))?;
                self.r.take(n)?;
            }
            ctype::LIST | ctype::SET => {
                let (et, count) = self.list_header()?;
                for _ in 0..count {
                    self.skip(et)?;
                }
            }
            ctype::STRUCT => {
                while let Some(f) = self.next_field()? {
                    let ct = f.ctype;
                    self.skip(ct)?;
                }
            }
            _ => return Err(err("skip: unknown ctype")),
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Run + iterate to green**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet thrift -- --nocapture`
Expected: 3 tests PASS. The asserted bytes are hand-derived from the Thrift compact spec (annotated inline); they must pass against this independent implementation — a genuine KAT. If a vector fails, re-derive it from the published spec by hand and FIX whichever side misread the spec (do not weaken the assertion to match code).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-parquet/src/thrift.rs
git commit -m "parquet: Thrift Compact Protocol reader (spec-derived byte KAT)"
```

---

### Task 3: Footer framing (`footer.rs`) — spec byte KAT

**Files:** Modify `crates/kessel-parquet/src/footer.rs`

- [ ] **Step 1: Failing KAT test**

Replace `footer.rs` with the impl (Step 3) + this test. The footer layout is the published Parquet spec: file = `PAR1` … `<FileMetaData thrift>` `[u32 LE metadata_len]` `PAR1`.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_framing_spec_kat() {
        // Minimal valid frame: header PAR1, a 3-byte fake metadata
        // blob [0xAA,0xBB,0xCC], len=3 little-endian, trailer PAR1.
        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        f.extend_from_slice(&3u32.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        let meta = metadata_slice(&f).unwrap();
        assert_eq!(meta, &[0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn footer_rejects_bad_magic_and_lying_len() {
        assert!(metadata_slice(b"NOPE....PAR1").is_err());
        // lying metadata_len far larger than the file
        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&[0x01]);
        f.extend_from_slice(&9_000_000u32.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        assert!(matches!(metadata_slice(&f), Err(crate::PqError::Bad(_))));
        // too short
        assert!(metadata_slice(b"PAR1").is_err());
        // bad trailer magic
        let mut g = Vec::new();
        g.extend_from_slice(b"PAR1");
        g.extend_from_slice(&[0x01]);
        g.extend_from_slice(&1u32.to_le_bytes());
        g.extend_from_slice(b"XXXX");
        assert!(metadata_slice(&g).is_err());
    }
}
```

- [ ] **Step 2: Run — expect compile failure** (`metadata_slice` undefined).
Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet footer -- --nocapture`

- [ ] **Step 3: Implement `footer.rs`**

```rust
//! Parquet file footer framing. Spec: `PAR1` <data...>
//! <FileMetaData> [u32 LE metadata_len] `PAR1`.
#![allow(dead_code)]

use crate::PqError;

const MAGIC: &[u8; 4] = b"PAR1";
/// Hard cap on the Thrift FileMetaData size (defensive — a real
/// metadata blob for a tiny mapped subset is KBs, not MBs).
const MAX_META: usize = 16 * 1024 * 1024;

fn bad(s: &str) -> PqError {
    PqError::Bad(s.to_string())
}

/// Return the `FileMetaData` thrift byte slice (validated framing).
pub fn metadata_slice(file: &[u8]) -> Result<&[u8], PqError> {
    if file.len() < 12 {
        return Err(bad("file too short for a Parquet footer"));
    }
    if &file[..4] != MAGIC {
        return Err(bad("missing PAR1 header magic"));
    }
    let n = file.len();
    if &file[n - 4..] != MAGIC {
        return Err(bad("missing PAR1 trailer magic"));
    }
    let len_pos = n - 8;
    let mlen = u32::from_le_bytes(
        file[len_pos..len_pos + 4].try_into().unwrap(),
    ) as usize;
    if mlen > MAX_META {
        return Err(bad("metadata_len exceeds cap"));
    }
    // metadata occupies [meta_start, len_pos); meta_start must be >= 4
    let meta_start = len_pos
        .checked_sub(mlen)
        .ok_or_else(|| bad("metadata_len larger than file"))?;
    if meta_start < 4 {
        return Err(bad("metadata overlaps header magic"));
    }
    Ok(&file[meta_start..len_pos])
}
```

- [ ] **Step 4: Run to green.** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet footer` → 2 pass.

- [ ] **Step 5: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-parquet/src/footer.rs
git commit -m "parquet: footer framing (PAR1 + LE metadata_len, bounds-checked, spec KAT)"
```

---

### Task 4: `FileMetaData` structs (`meta.rs`) via thrift.rs

Parquet's `parquet.thrift` field IDs are the published, stable contract — these are the spec authority. Only the fields OBJ-2a needs are read; the rest are `skip`ped.

**Files:** Modify `crates/kessel-parquet/src/meta.rs`

- [ ] **Step 1: Failing test (decode a hand-built spec-faithful FileMetaData blob)**

Replace `meta.rs` with the impl (Step 3) + this test. The blob is assembled with `thrift.rs`'s encoder-inverse by hand from `parquet.thrift` field IDs (documented inline) — it exercises the decoder against an independently-constructed structure, and the primitive correctness is already KAT-pinned in Task 2.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // Helpers to hand-build a compact-thrift struct (spec-faithful,
    // independent of the decoder under test).
    fn uv(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 { out.push(b); break; } else { out.push(b | 0x80); }
        }
    }
    fn zz(v: i64) -> u64 { ((v << 1) ^ (v >> 63)) as u64 }

    #[test]
    fn decode_minimal_filemetadata() {
        // parquet.thrift FileMetaData: 1:i32 version, 2:list<SchemaElement>
        // schema, 3:i64 num_rows, 4:list<RowGroup> row_groups.
        // SchemaElement: 1:Type type, 4:RepetitionType repetition_type,
        // 5:i32 num_children, 8(name moved) — actual ids: 1 type,
        // 3 repetition_type, 4 name, 5 num_children (per parquet.thrift).
        // RowGroup: 1:list<ColumnChunk> columns, 2:i64 total_byte_size,
        // 3:i64 num_rows. ColumnChunk: 3:ColumnMetaData meta_data.
        // ColumnMetaData: 1:Type type, 2:list<Encoding> encodings,
        // 3:list<string> path_in_schema, 4:CompressionCodec codec,
        // 5:i64 num_values, 9:i64 data_page_offset.
        //
        // We assert: version, num_rows, one schema leaf "id" Type=INT64
        // REQUIRED, one row group with one ColumnChunk for "id".
        // (Bytes hand-assembled below; field-header = (delta<<4)|ctype.)
        let mut b = Vec::new();
        // FileMetaData struct:
        // f1 i32 version=1 : header (1<<4)|5=0x15, zz(1)=2
        b.push(0x15); uv(&mut b, zz(1));
        // f2 list<struct> schema, 2 elements (root group + leaf):
        //   header (1<<4)|9=0x19 ; list size/type byte (2<<4)|12=0x2c
        b.push(0x19); b.push(0x2c);
        //   schema[0] root group: name f4="root", num_children f5=1
        //     name: header (4<<4)|8=0x48, len4 "root"
        b.push(0x48); uv(&mut b, 4); b.extend_from_slice(b"root");
        //     num_children: delta to f5 from f4 =1 -> (1<<4)|5=0x15, zz(1)=2
        b.push(0x15); uv(&mut b, zz(1));
        b.push(0x00); // stop schema[0]
        //   schema[1] leaf "id": type f1=INT64(2), repetition f3=REQUIRED(0),
        //     name f4="id"
        //     type: (1<<4)|5=0x15, zz(2)=4   (Type enum INT64=2)
        b.push(0x15); uv(&mut b, zz(2));
        //     repetition_type: delta f1->f3 =2 -> (2<<4)|5=0x25, zz(0)=0  (REQUIRED=0)
        b.push(0x25); uv(&mut b, zz(0));
        //     name: delta f3->f4 =1 -> (1<<4)|8=0x18, len2 "id"
        b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
        b.push(0x00); // stop schema[1]
        // f3 i64 num_rows=2 : delta f2->f3 =1 -> (1<<4)|6=0x16, zz(2)=4
        b.push(0x16); uv(&mut b, zz(2));
        // f4 list<RowGroup> row_groups, 1 element :
        //   delta f3->f4=1 -> (1<<4)|9=0x19 ; list (1<<4)|12=0x1c
        b.push(0x19); b.push(0x1c);
        //   RowGroup: f1 list<ColumnChunk> columns (1 elem),
        //     f3 i64 num_rows=2
        //     f1 list: (1<<4)|9=0x19 ; list (1<<4)|12=0x1c
        b.push(0x19); b.push(0x1c);
        //       ColumnChunk: f3 ColumnMetaData meta_data
        //         delta 0->3 =3 -> (3<<4)|12=0x3c
        b.push(0x3c);
        //         ColumnMetaData: f1 Type=INT64(2), f2 list<Encoding>[PLAIN(0)],
        //           f3 list<string> path=["id"], f4 codec=UNCOMPRESSED(0),
        //           f5 i64 num_values=2, f9 i64 data_page_offset=4
        //         f1 type: (1<<4)|5=0x15 zz(2)=4
        b.push(0x15); uv(&mut b, zz(2));
        //         f2 encodings list<i32> 1 elem PLAIN(0):
        //           delta1 -> (1<<4)|9=0x19 ; list (1<<4)|5=0x15 ; zz(0)=0
        b.push(0x19); b.push(0x15); uv(&mut b, zz(0));
        //         f3 path_in_schema list<string> ["id"]:
        //           delta1 -> (1<<4)|9=0x19 ; list (1<<4)|8=0x18 ; len2 "id"
        b.push(0x19); b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
        //         f4 codec UNCOMPRESSED(0): delta1 -> (1<<4)|5=0x15 zz(0)=0
        b.push(0x15); uv(&mut b, zz(0));
        //         f5 num_values=2: delta1 -> (1<<4)|6=0x16 zz(2)=4
        b.push(0x16); uv(&mut b, zz(2));
        //         f9 data_page_offset=4: delta f5->f9 =4 -> (4<<4)|6=0x46 zz(4)=8
        b.push(0x46); uv(&mut b, zz(4));
        b.push(0x00); // stop ColumnMetaData
        b.push(0x00); // stop ColumnChunk
        //   RowGroup f3 num_rows=2 : last id in RG was 1 (columns),
        //     delta 1->3 =2 -> (2<<4)|6=0x26 zz(2)=4
        b.push(0x26); uv(&mut b, zz(2));
        b.push(0x00); // stop RowGroup
        b.push(0x00); // stop FileMetaData

        let md = FileMetaData::decode(&b).expect("decode");
        assert_eq!(md.version, 1);
        assert_eq!(md.num_rows, 2);
        assert_eq!(md.leaves.len(), 1);
        let leaf = &md.leaves[0];
        assert_eq!(leaf.name, "id");
        assert_eq!(leaf.ptype, Type::Int64);
        assert_eq!(leaf.repetition, Repetition::Required);
        assert_eq!(md.row_groups.len(), 1);
        let cc = &md.row_groups[0].columns[0];
        assert_eq!(cc.path, vec!["id".to_string()]);
        assert_eq!(cc.ptype, Type::Int64);
        assert_eq!(cc.codec, Codec::Uncompressed);
        assert_eq!(cc.encodings, vec![Encoding::Plain]);
        assert_eq!(cc.num_values, 2);
        assert_eq!(cc.data_page_offset, 4);
    }
}
```

- [ ] **Step 2: Run — expect compile failure.**
Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet meta -- --nocapture`

- [ ] **Step 3: Implement `meta.rs`**

```rust
//! Parquet `FileMetaData` (the subset OBJ-2a needs). Field IDs from
//! the published `parquet.thrift`. Unknown fields are skipped.
#![allow(dead_code)]

use crate::thrift::{ctype, StructReader};
use crate::PqError;

fn bad(s: &str) -> PqError {
    PqError::Bad(s.to_string())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Type {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray,
    Other(i32),
}
impl Type {
    fn from_i32(v: i32) -> Type {
        match v {
            0 => Type::Boolean,
            1 => Type::Int32,
            2 => Type::Int64,
            3 => Type::Int96,
            4 => Type::Float,
            5 => Type::Double,
            6 => Type::ByteArray,
            7 => Type::FixedLenByteArray,
            o => Type::Other(o),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Repetition {
    Required,
    Optional,
    Repeated,
    Other(i32),
}
impl Repetition {
    fn from_i32(v: i32) -> Repetition {
        match v {
            0 => Repetition::Required,
            1 => Repetition::Optional,
            2 => Repetition::Repeated,
            o => Repetition::Other(o),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    Uncompressed,
    Other(i32),
}
impl Codec {
    fn from_i32(v: i32) -> Codec {
        if v == 0 { Codec::Uncompressed } else { Codec::Other(v) }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    Plain,
    Other(i32),
}
impl Encoding {
    fn from_i32(v: i32) -> Encoding {
        if v == 0 { Encoding::Plain } else { Encoding::Other(v) }
    }
}

#[derive(Clone, Debug)]
pub struct SchemaLeaf {
    pub name: String,
    pub ptype: Type,
    pub repetition: Repetition,
}

#[derive(Clone, Debug)]
pub struct ColumnChunk {
    pub path: Vec<String>,
    pub ptype: Type,
    pub codec: Codec,
    pub encodings: Vec<Encoding>,
    pub num_values: i64,
    pub data_page_offset: i64,
}

#[derive(Clone, Debug)]
pub struct RowGroup {
    pub columns: Vec<ColumnChunk>,
    pub num_rows: i64,
}

#[derive(Clone, Debug)]
pub struct FileMetaData {
    pub version: i32,
    pub num_rows: i64,
    /// Flat leaf schema elements (root group element with
    /// num_children>0 is excluded; only true leaves kept).
    pub leaves: Vec<SchemaLeaf>,
    pub row_groups: Vec<RowGroup>,
}

impl FileMetaData {
    pub fn decode(bytes: &[u8]) -> Result<FileMetaData, PqError> {
        let mut s = StructReader::new(bytes);
        let mut version = 0i32;
        let mut num_rows = 0i64;
        let mut leaves: Vec<SchemaLeaf> = Vec::new();
        let mut row_groups: Vec<RowGroup> = Vec::new();
        while let Some(f) = s.next_field()? {
            match f.id {
                1 => version = s.read_i32(&f)?,
                2 => {
                    let (et, count) = s.list_header()?;
                    if et != ctype::STRUCT {
                        return Err(bad("schema list type"));
                    }
                    for _ in 0..count {
                        if let Some(le) = decode_schema_element(&mut s)? {
                            leaves.push(le);
                        }
                    }
                }
                3 => num_rows = s.read_i64(&f)?,
                4 => {
                    let (et, count) = s.list_header()?;
                    if et != ctype::STRUCT {
                        return Err(bad("row_groups list type"));
                    }
                    for _ in 0..count {
                        row_groups.push(decode_row_group(&mut s)?);
                    }
                }
                _ => s.skip(f.ctype)?,
            }
        }
        Ok(FileMetaData { version, num_rows, leaves, row_groups })
    }
}

/// Returns Some(leaf) for a true leaf (num_children == 0), None for
/// a group element (root / nested) — OBJ-2a only consumes leaves;
/// nested groups are detected later via repetition checks.
fn decode_schema_element(
    s: &mut StructReader,
) -> Result<Option<SchemaLeaf>, PqError> {
    let mut ptype = Type::Other(-1);
    let mut repetition = Repetition::Required;
    let mut name = String::new();
    let mut num_children = 0i32;
    let mut saw_type = false;
    while let Some(f) = s.next_field()? {
        match f.id {
            1 => {
                ptype = Type::from_i32(s.read_i32(&f)?);
                saw_type = true;
            }
            3 => repetition = Repetition::from_i32(s.read_i32(&f)?),
            4 => {
                name = String::from_utf8_lossy(s.read_binary(&f)?)
                    .into_owned()
            }
            5 => num_children = s.read_i32(&f)?,
            _ => s.skip(f.ctype)?,
        }
    }
    if num_children > 0 || !saw_type {
        return Ok(None); // group element, not a leaf
    }
    Ok(Some(SchemaLeaf { name, ptype, repetition }))
}

fn decode_row_group(
    s: &mut StructReader,
) -> Result<RowGroup, PqError> {
    let mut columns: Vec<ColumnChunk> = Vec::new();
    let mut num_rows = 0i64;
    while let Some(f) = s.next_field()? {
        match f.id {
            1 => {
                let (et, count) = s.list_header()?;
                if et != ctype::STRUCT {
                    return Err(bad("columns list type"));
                }
                for _ in 0..count {
                    columns.push(decode_column_chunk(s)?);
                }
            }
            3 => num_rows = s.read_i64(&f)?,
            _ => s.skip(f.ctype)?,
        }
    }
    Ok(RowGroup { columns, num_rows })
}

fn decode_column_chunk(
    s: &mut StructReader,
) -> Result<ColumnChunk, PqError> {
    let mut out: Option<ColumnChunk> = None;
    while let Some(f) = s.next_field()? {
        match f.id {
            3 => out = Some(decode_column_meta(s)?),
            _ => s.skip(f.ctype)?,
        }
    }
    out.ok_or_else(|| bad("ColumnChunk missing meta_data"))
}

fn decode_column_meta(
    s: &mut StructReader,
) -> Result<ColumnChunk, PqError> {
    let mut ptype = Type::Other(-1);
    let mut codec = Codec::Uncompressed;
    let mut encodings: Vec<Encoding> = Vec::new();
    let mut path: Vec<String> = Vec::new();
    let mut num_values = 0i64;
    let mut data_page_offset = 0i64;
    while let Some(f) = s.next_field()? {
        match f.id {
            1 => ptype = Type::from_i32(s.read_i32(&f)?),
            2 => {
                let (et, count) = s.list_header()?;
                for _ in 0..count {
                    if et != ctype::I32 && et != ctype::I8
                        && et != ctype::I16
                    {
                        return Err(bad("encodings list type"));
                    }
                    let v = i32::try_from(s.reader().ivarint()?)
                        .map_err(|_| bad("encoding range"))?;
                    encodings.push(Encoding::from_i32(v));
                }
            }
            3 => {
                let (et, count) = s.list_header()?;
                if et != ctype::BINARY {
                    return Err(bad("path list type"));
                }
                for _ in 0..count {
                    let n = usize::try_from(s.reader().uvarint()?)
                        .map_err(|_| bad("path len"))?;
                    let seg = s.reader().take(n)?;
                    path.push(
                        String::from_utf8_lossy(seg).into_owned(),
                    );
                }
            }
            4 => codec = Codec::from_i32(s.read_i32(&f)?),
            5 => num_values = s.read_i64(&f)?,
            9 => data_page_offset = s.read_i64(&f)?,
            _ => s.skip(f.ctype)?,
        }
    }
    Ok(ColumnChunk {
        path,
        ptype,
        codec,
        encodings,
        num_values,
        data_page_offset,
    })
}

/// V1 DataPageHeader (PageHeader: 1:PageType type, 3:i32
/// uncompressed_page_size, 5:DataPageHeader data_page_header;
/// DataPageHeader: 1:i32 num_values, 2:Encoding encoding).
#[derive(Clone, Debug)]
pub struct PageHeader {
    pub page_type: i32,
    pub uncompressed_size: i32,
    pub compressed_size: i32,
    pub dp_num_values: i32,
    pub dp_encoding: i32,
}

pub fn decode_page_header(
    bytes: &[u8],
) -> Result<(PageHeader, usize), PqError> {
    let mut s = StructReader::new(bytes);
    let mut ph = PageHeader {
        page_type: -1,
        uncompressed_size: 0,
        compressed_size: 0,
        dp_num_values: 0,
        dp_encoding: -1,
    };
    while let Some(f) = s.next_field()? {
        match f.id {
            1 => ph.page_type = s.read_i32(&f)?,
            3 => ph.uncompressed_size = s.read_i32(&f)?,
            4 => ph.compressed_size = s.read_i32(&f)?,
            5 => {
                // nested DataPageHeader struct
                while let Some(g) = s.next_field()? {
                    match g.id {
                        1 => ph.dp_num_values = s.read_i32(&g)?,
                        2 => ph.dp_encoding = s.read_i32(&g)?,
                        _ => s.skip(g.ctype)?,
                    }
                }
            }
            _ => s.skip(f.ctype)?,
        }
    }
    Ok((ph, s.reader_pos()))
}
```

Add to `thrift.rs` `impl<'a> StructReader<'a>` a `pub fn reader_pos(&self) -> usize { self.r.p }` (and make `Reader.p` reachable: add `pub fn pos(&self) -> usize { self.p }` on `Reader` and have `reader_pos` return `self.r.pos()`), so `decode_page_header` can report how many bytes the header consumed (the page data follows immediately after).

- [ ] **Step 4: Run to green.** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet meta -- --nocapture` → PASS. If the hand-assembled blob disagrees with the decoder, re-derive the field-header bytes from `parquet.thrift` field IDs + the Thrift compact spec (Task 2 already pins the primitives); fix whichever side misread the spec.

- [ ] **Step 5: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-parquet/src/meta.rs crates/kessel-parquet/src/thrift.rs
git commit -m "parquet: FileMetaData + PageHeader decode (parquet.thrift field IDs)"
```

---

### Task 5: PLAIN page decode (`plain.rs`) — spec byte KAT per physical type

**Files:** Modify `crates/kessel-parquet/src/plain.rs`

- [ ] **Step 1: Failing KAT test** (PLAIN layout is the published Parquet spec: INT32/INT64 little-endian; FLOAT/DOUBLE IEEE-754 LE; BOOLEAN bit-packed LSB-first; BYTE_ARRAY = `[u32 LE len][bytes]`).

Replace `plain.rs` with the impl (Step 3) + this test:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::Type;
    use crate::PqValue;

    #[test]
    fn plain_decode_spec_kat() {
        // INT64: values 7, -2  -> 7i64 LE, (-2)i64 LE
        let mut b = Vec::new();
        b.extend_from_slice(&7i64.to_le_bytes());
        b.extend_from_slice(&(-2i64).to_le_bytes());
        assert_eq!(
            decode_plain(&b, Type::Int64, 2).unwrap(),
            vec![PqValue::I64(7), PqValue::I64(-2)]
        );
        // INT32: 1, 1000
        let mut c = Vec::new();
        c.extend_from_slice(&1i32.to_le_bytes());
        c.extend_from_slice(&1000i32.to_le_bytes());
        assert_eq!(
            decode_plain(&c, Type::Int32, 2).unwrap(),
            vec![PqValue::I64(1), PqValue::I64(1000)]
        );
        // DOUBLE: 1.5, -0.25 (exact in f64)
        let mut d = Vec::new();
        d.extend_from_slice(&1.5f64.to_le_bytes());
        d.extend_from_slice(&(-0.25f64).to_le_bytes());
        assert_eq!(
            decode_plain(&d, Type::Double, 2).unwrap(),
            vec![PqValue::F64(1.5), PqValue::F64(-0.25)]
        );
        // FLOAT: 2.0
        let e = 2.0f32.to_le_bytes().to_vec();
        assert_eq!(
            decode_plain(&e, Type::Float, 1).unwrap(),
            vec![PqValue::F64(2.0)]
        );
        // BOOLEAN bit-packed LSB-first: true,false,true -> 0b00000101
        assert_eq!(
            decode_plain(&[0b0000_0101], Type::Boolean, 3).unwrap(),
            vec![PqValue::Bool(true), PqValue::Bool(false), PqValue::Bool(true)]
        );
        // BYTE_ARRAY: "hi","x" -> [2,0,0,0]"hi"[1,0,0,0]"x"
        let mut g = Vec::new();
        g.extend_from_slice(&2u32.to_le_bytes()); g.extend_from_slice(b"hi");
        g.extend_from_slice(&1u32.to_le_bytes()); g.extend_from_slice(b"x");
        assert_eq!(
            decode_plain(&g, Type::ByteArray, 2).unwrap(),
            vec![PqValue::Bytes(b"hi".to_vec()), PqValue::Bytes(b"x".to_vec())]
        );
    }

    #[test]
    fn plain_truncated_is_typed_error() {
        assert!(decode_plain(&[0u8; 3], Type::Int64, 1).is_err());
        let mut g = Vec::new();
        g.extend_from_slice(&99u32.to_le_bytes()); // lying length
        g.extend_from_slice(b"hi");
        assert!(decode_plain(&g, Type::ByteArray, 1).is_err());
    }
}
```

- [ ] **Step 2: Run — expect compile failure** (`decode_plain` undefined).

- [ ] **Step 3: Implement `plain.rs`**

```rust
//! PLAIN-encoding page decode (Apache Parquet "Data Pages"/PLAIN).
//! INT32/INT64 LE; FLOAT/DOUBLE IEEE-754 LE; BOOLEAN bit-packed
//! LSB-first; BYTE_ARRAY = [u32 LE len][bytes]. Bounds-checked.
#![allow(dead_code)]

use crate::meta::Type;
use crate::{PqError, PqValue};

fn bad(s: &str) -> PqError {
    PqError::Bad(s.to_string())
}

pub fn decode_plain(
    data: &[u8],
    ptype: Type,
    count: usize,
) -> Result<Vec<PqValue>, PqError> {
    let mut out = Vec::with_capacity(count);
    match ptype {
        Type::Int32 => {
            let need = count.checked_mul(4).ok_or_else(|| bad("int32 ovf"))?;
            let s = data.get(..need).ok_or_else(|| bad("int32 truncated"))?;
            for ch in s.chunks_exact(4) {
                out.push(PqValue::I64(
                    i32::from_le_bytes(ch.try_into().unwrap()) as i64,
                ));
            }
        }
        Type::Int64 => {
            let need = count.checked_mul(8).ok_or_else(|| bad("int64 ovf"))?;
            let s = data.get(..need).ok_or_else(|| bad("int64 truncated"))?;
            for ch in s.chunks_exact(8) {
                out.push(PqValue::I64(i64::from_le_bytes(
                    ch.try_into().unwrap(),
                )));
            }
        }
        Type::Float => {
            let need = count.checked_mul(4).ok_or_else(|| bad("f32 ovf"))?;
            let s = data.get(..need).ok_or_else(|| bad("f32 truncated"))?;
            for ch in s.chunks_exact(4) {
                out.push(PqValue::F64(
                    f32::from_le_bytes(ch.try_into().unwrap()) as f64,
                ));
            }
        }
        Type::Double => {
            let need = count.checked_mul(8).ok_or_else(|| bad("f64 ovf"))?;
            let s = data.get(..need).ok_or_else(|| bad("f64 truncated"))?;
            for ch in s.chunks_exact(8) {
                out.push(PqValue::F64(f64::from_le_bytes(
                    ch.try_into().unwrap(),
                )));
            }
        }
        Type::Boolean => {
            let need = count.div_ceil(8);
            let s = data.get(..need).ok_or_else(|| bad("bool truncated"))?;
            for i in 0..count {
                let byte = s[i / 8];
                out.push(PqValue::Bool((byte >> (i % 8)) & 1 == 1));
            }
        }
        Type::ByteArray => {
            let mut p = 0usize;
            for _ in 0..count {
                let lb = data
                    .get(p..p + 4)
                    .ok_or_else(|| bad("byte_array len truncated"))?;
                let len = u32::from_le_bytes(lb.try_into().unwrap())
                    as usize;
                p += 4;
                let v = data
                    .get(p..p.checked_add(len)
                        .ok_or_else(|| bad("byte_array len ovf"))?)
                    .ok_or_else(|| bad("byte_array data truncated"))?;
                out.push(PqValue::Bytes(v.to_vec()));
                p += len;
            }
        }
        other => {
            return Err(PqError::Unsupported(format!(
                "physical type {other:?} (OBJ-2c)"
            )))
        }
    }
    Ok(out)
}
```

- [ ] **Step 4: Run to green.** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet plain -- --nocapture` → 2 pass.

- [ ] **Step 5: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-parquet/src/plain.rs
git commit -m "parquet: PLAIN page decode per physical type (spec byte KAT, bounds-checked)"
```

---

### Task 6: `extract()` orchestration + support-matrix gate (`lib.rs`)

**Files:** Modify `crates/kessel-parquet/src/lib.rs`

- [ ] **Step 1: Failing test** — assemble a whole spec-faithful Parquet file in-memory (footer + the Task-4 metadata blob with a real `data_page_offset` + a V1 data-page header + a PLAIN page) and assert `extract` returns the mapped column's values; plus the support-matrix rejections.

Add to `lib.rs` a `#[cfg(test)] mod tests` that builds a one-column (`id` INT64 REQUIRED, 2 rows = 7,-2), one-row-group, PLAIN, UNCOMPRESSED, V1 file using the same hand-encoder helpers as Task 4 (copy the `uv`/`zz` helpers + the metadata builder, set `data_page_offset` to the actual byte offset of the page header you place after the `PAR1` header), then:

```rust
        let rows = extract(&file, &["id"]).expect("extract");
        assert_eq!(rows, vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]);
        // mapped subset + order: a 2-col file, ask only the 2nd then 1st
        // (covered by a second hand-built 2-col fixture in this test).
        // Unsupported gates:
        assert!(matches!(extract(&dict_file, &["id"]),
            Err(PqError::Unsupported(_))));      // encoding != PLAIN
        assert!(matches!(extract(&snappy_file, &["id"]),
            Err(PqError::Unsupported(_))));      // codec != UNCOMPRESSED
        assert!(matches!(extract(&optional_file, &["id"]),
            Err(PqError::Unsupported(_))));      // repetition != REQUIRED
        assert!(matches!(extract(&good_file, &["missing"]),
            Err(PqError::Bad(_))));              // column not in schema
```

(The implementer constructs `dict_file`/`snappy_file`/`optional_file` by toggling exactly the one metadata field — encoding list element, codec, repetition_type — in the spec-faithful builder; this proves the gate triggers on a single-attribute change, independent of the reader.)

- [ ] **Step 2: Run — expect failure** (`extract` is the Task-1 stub).

- [ ] **Step 3: Implement `extract` in `lib.rs`** (replace the stub body):

```rust
pub fn extract(
    bytes: &[u8],
    wanted: &[&str],
) -> Result<Vec<Vec<PqValue>>, PqError> {
    let md_bytes = footer::metadata_slice(bytes)?;
    let md = meta::FileMetaData::decode(md_bytes)?;

    // Resolve each wanted name to its leaf; enforce REQUIRED + flat.
    for w in wanted {
        let leaf = md
            .leaves
            .iter()
            .find(|l| &l.name == w)
            .ok_or_else(|| {
                PqError::Bad(format!("column `{w}` not found in Parquet schema"))
            })?;
        if leaf.repetition != meta::Repetition::Required {
            return Err(PqError::Unsupported(
                "OPTIONAL/REPEATED columns: OBJ-2b".into(),
            ));
        }
        match leaf.ptype {
            meta::Type::Boolean
            | meta::Type::Int32
            | meta::Type::Int64
            | meta::Type::Float
            | meta::Type::Double
            | meta::Type::ByteArray => {}
            t => {
                return Err(PqError::Unsupported(format!(
                    "physical type {t:?}: OBJ-2c"
                )))
            }
        }
    }

    // Per wanted column: concatenate its values across all row groups.
    let mut cols: Vec<Vec<PqValue>> =
        wanted.iter().map(|_| Vec::new()).collect();
    for rg in &md.row_groups {
        for (ci, w) in wanted.iter().enumerate() {
            let cc = rg
                .columns
                .iter()
                .find(|c| c.path.last().map(|s| s.as_str()) == Some(*w))
                .ok_or_else(|| {
                    PqError::Bad(format!(
                        "row group missing column `{w}`"
                    ))
                })?;
            if cc.codec != meta::Codec::Uncompressed {
                return Err(PqError::Unsupported(
                    "compression: OBJ-2b/2c".into(),
                ));
            }
            if cc.encodings.iter().any(|e| *e != meta::Encoding::Plain) {
                return Err(PqError::Unsupported(
                    "non-PLAIN encoding (dictionary/RLE/DELTA): OBJ-2b"
                        .into(),
                ));
            }
            let off = usize::try_from(cc.data_page_offset)
                .map_err(|_| PqError::Bad("page offset range".into()))?;
            let page_region = bytes
                .get(off..)
                .ok_or_else(|| PqError::Bad("page offset past EOF".into()))?;
            let (ph, hdr_len) = meta::decode_page_header(page_region)?;
            // V1 data page only (PageType DATA_PAGE == 0).
            if ph.page_type != 0 {
                return Err(PqError::Unsupported(
                    "non-V1 / dictionary / index page: OBJ-2b".into(),
                ));
            }
            if ph.dp_encoding != 0 {
                return Err(PqError::Unsupported(
                    "data page encoding != PLAIN: OBJ-2b".into(),
                ));
            }
            let n = usize::try_from(ph.dp_num_values)
                .map_err(|_| PqError::Bad("num_values range".into()))?;
            let dstart = off
                .checked_add(hdr_len)
                .ok_or_else(|| PqError::Bad("page hdr len ovf".into()))?;
            let dend = dstart
                .checked_add(
                    usize::try_from(ph.uncompressed_size)
                        .map_err(|_| PqError::Bad("page size range".into()))?,
                )
                .ok_or_else(|| PqError::Bad("page size ovf".into()))?;
            let page = bytes
                .get(dstart..dend)
                .ok_or_else(|| PqError::Bad("page data truncated".into()))?;
            let vals = plain::decode_plain(page, cc.ptype, n)?;
            cols[ci].extend(vals);
        }
    }

    // Transpose columns → rows; all columns must have equal length.
    let nrows = cols.first().map(|c| c.len()).unwrap_or(0);
    if cols.iter().any(|c| c.len() != nrows) {
        return Err(PqError::Bad("column length mismatch".into()));
    }
    let mut rows = Vec::with_capacity(nrows);
    for r in 0..nrows {
        rows.push(cols.iter().map(|c| c[r].clone()).collect());
    }
    Ok(rows)
}
```

- [ ] **Step 4: Run to green.** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet -- --nocapture` → all green (thrift/footer/meta/plain/lib). Fix derivation mismatches against the public spec, never weaken assertions.

- [ ] **Step 5: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-parquet/src/lib.rs
git commit -m "parquet: extract() orchestration + OBJ-2a support-matrix gate"
```

---

### Task 7: Real Parquet fixture(s) + README (real-writer-first, BLOCK-not-fake)

**Files:** Create `crates/kessel-parquet/tests/fixtures/{*.parquet,README.md}` + `crates/kessel-parquet/tests/fixture_roundtrip.rs`

- [ ] **Step 1: Try a real external Parquet writer FIRST**

Run, in order, until one succeeds:
- `cd /c/Users/ihass/KesselDB && python -m pip install --quiet pyarrow 2>&1 | tail -1 && python -c "import pyarrow; print('PYARROW', pyarrow.__version__)"`
- if that fails: `python -m pip install --quiet duckdb 2>&1 | tail -1 && python -c "import duckdb; print('DUCKDB', duckdb.__version__)"`

If a real writer installs, generate the fixtures with it (this is the preferred, unambiguous path — a real external Parquet producer):

```bash
cd /c/Users/ihass/KesselDB/crates/kessel-parquet/tests/fixtures
python - <<'PY'
import pyarrow as pa, pyarrow.parquet as pq
t = pa.table({'id': pa.array([7,-2,100], pa.int64()),
              'name': pa.array(['hi','x','zed'], pa.string()),
              'flag': pa.array([True,False,True], pa.bool_()),
              'score': pa.array([1.5,-0.25,3.0], pa.float64())})
pq.write_table(t, 'flat_required.parquet', version='1.0',
               use_dictionary=False, compression='NONE',
               data_page_version='1.0')
# multi-row-group
pq.write_table(t, 'flat_multirg.parquet', version='1.0',
               use_dictionary=False, compression='NONE',
               data_page_version='1.0', row_group_size=2)
PY
```

- [ ] **Step 2: If NO external writer can be installed (offline)** — build the fixtures with an **independent spec-faithful generator** committed at `crates/kessel-parquet/tests/fixture_gen.rs` (a `#[cfg(test)]` writer authored from the published Parquet/Thrift spec, NOT by calling the reader). It writes the same logical table as Step 1. The non-self-reference anchor is that Tasks 2–5 already KAT-pin the Thrift/footer/PLAIN primitives against hand-computed public-spec bytes; the generator composes those spec-pinned primitives. **If you cannot establish the fixture via a real writer AND cannot author a spec-faithful generator whose primitives are the Task 2–5 spec-KAT'd ones, report BLOCKED — do not hand-fake a file only the reader can parse.**

- [ ] **Step 3: Fixtures README** — `crates/kessel-parquet/tests/fixtures/README.md`: which path produced them (pyarrow/duckdb version + exact command, OR "spec-faithful generator `fixture_gen.rs` — primitives KAT-pinned to Apache Thrift/Parquet spec in thrift/footer/plain tests"); the exact logical rows each fixture encodes; the OBJ-2a producer constraints (`version='1.0'`, `use_dictionary=False`, `compression='NONE'`, `data_page_version='1.0'`); note these are test data, not security-sensitive.

- [ ] **Step 4: Round-trip test** `crates/kessel-parquet/tests/fixture_roundtrip.rs`:

```rust
use kessel_parquet::{extract, PqValue};
const FLAT: &[u8] = include_bytes!("fixtures/flat_required.parquet");
const MRG: &[u8] = include_bytes!("fixtures/flat_multirg.parquet");

#[test]
fn fixture_flat_required_decodes_expected_rows() {
    let rows = extract(FLAT, &["id", "name"]).unwrap();
    assert_eq!(rows[0], vec![PqValue::I64(7), PqValue::Bytes(b"hi".to_vec())]);
    assert_eq!(rows[1], vec![PqValue::I64(-2), PqValue::Bytes(b"x".to_vec())]);
    assert_eq!(rows[2], vec![PqValue::I64(100), PqValue::Bytes(b"zed".to_vec())]);
    // subset + reordering
    let r2 = extract(FLAT, &["flag", "score"]).unwrap();
    assert_eq!(r2[0], vec![PqValue::Bool(true), PqValue::F64(1.5)]);
}

#[test]
fn fixture_multi_row_group_concatenates() {
    let rows = extract(MRG, &["id"]).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2], vec![PqValue::I64(100)]);
}
```

- [ ] **Step 5: Run** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet --test fixture_roundtrip -- --nocapture` → PASS. Report WHICH fixture path was used (real writer vs spec-faithful generator) + the tool/version.

- [ ] **Step 6: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-parquet/tests
git commit -m "parquet: real/spec-faithful Parquet fixtures + round-trip (BLOCK-not-fake discipline)"
```

---

### Task 8: `kessel-fetch` `Format::Parquet` + `rows_from_body` arm + `pq_to_cell`

**Files:** Modify `crates/kessel-fetch/Cargo.toml`, `crates/kessel-fetch/src/lib.rs`; Create `crates/kessel-fetch/tests/parquet_decode.rs`

- [ ] **Step 1: Cargo wiring** — in `crates/kessel-fetch/Cargo.toml` add `kessel-parquet = { path = "../kessel-parquet", optional = true }` under `[dependencies]`, and add `dep:kessel-parquet` to the existing `object-store` feature (`object-store = ["tls", "dep:kessel-objstore", "dep:kessel-parquet"]` — append, keep the others).

- [ ] **Step 2: Failing feature-gated test** — `crates/kessel-fetch/tests/parquet_decode.rs`:

```rust
//! rows_from_body(Format::Parquet) over the kessel-parquet fixture.
//! Only compiled with --features object-store.
#![cfg(feature = "object-store")]

use kessel_catalog::FieldKind;
use kessel_fetch::{rows_from_body_for_test, ColumnMap, Format};

const FLAT: &[u8] =
    include_bytes!("../../kessel-parquet/tests/fixtures/flat_required.parquet");

#[test]
fn parquet_rows_coerce_through_existing_path() {
    let cols = vec![
        ColumnMap { name: "id".into(), kind: FieldKind::U64, source: "id".into() },
        ColumnMap { name: "name".into(), kind: FieldKind::Char(8), source: "name".into() },
    ];
    let rows = rows_from_body_for_test(FLAT, Format::Parquet, &cols, None).unwrap();
    assert_eq!(rows[0][0], 7u64.to_le_bytes().to_vec());
    assert_eq!(&rows[0][1][..2], b"hi");
}
```

(If a `rows_from_body` test shim doesn't already exist, add `#[doc(hidden)] pub fn rows_from_body_for_test(b:&[u8],f:Format,c:&[ColumnMap],rp:Option<&str>)->Result<Vec<Vec<Vec<u8>>>,FetchError> { rows_from_body(b,f,c,rp) }` next to `rows_from_body` — mirrors the existing `http_get_resp_for_test` shim pattern.)

- [ ] **Step 3: Implement** — in `crates/kessel-fetch/src/lib.rs`:
  - Add `Parquet` to `pub enum Format` (it derives `Clone,Copy,Debug,PartialEq,Eq` — fine).
  - Add the `rows_from_body` arm (the `match format` is exhaustive — the compiler forces this):

```rust
        #[cfg(feature = "object-store")]
        Format::Parquet => {
            let names: Vec<&str> =
                cols.iter().map(|c| c.source.as_str()).collect();
            let pv = kessel_parquet::extract(body, &names)
                .map_err(|e| FetchError::Parse(e.to_string()))?;
            pv.into_iter()
                .map(|row| row.into_iter().map(pq_to_cell).collect())
                .collect()
        }
        #[cfg(not(feature = "object-store"))]
        Format::Parquet => {
            return Err(FetchError::Parse(
                "FORMAT PARQUET requires the object-store build".into(),
            ))
        }
```

  - Add the mapper (place near `rows_from_body`):

```rust
#[cfg(feature = "object-store")]
fn pq_to_cell(v: kessel_parquet::PqValue) -> json::Cell {
    use kessel_parquet::PqValue::*;
    match v {
        Null => json::Cell::Null,
        Bool(b) => json::Cell::Bool(b),
        // Integers/floats rendered to the SAME textual Cell the JSON
        // path produces for a number, so coerce::to_field_bytes is
        // byte-identical regardless of source format.
        I64(i) => json::Cell::Text(i.to_string()),
        F64(f) => json::Cell::Text(json::canonical_f64(f)),
        Bytes(b) => json::Cell::Text(
            String::from_utf8_lossy(&b).into_owned(),
        ),
    }
}
```

  - In `json.rs` add `pub(crate) fn canonical_f64(f: f64) -> String` returning EXACTLY the textual form a JSON number `Cell::Text` already carries for the same value (read how `json.rs` currently stringifies `Json::Num` — it stores the original number token as `Cell::Text(n.clone())`; for Parquet there is no source token, so define `canonical_f64` to format with Rust's default `{}` for `f64` which is the shortest round-trip representation — deterministic, locale-free; integers-valued floats print without a trailing `.0` only if that matches the JSON path — to be safe, document that Parquet F64 uses `format!("{f}")` and the coerce path parses it the same as a JSON numeric string). Add a unit test in `json.rs` asserting `canonical_f64(1.5)=="1.5"`, `canonical_f64(3.0)=="3"` or `"3"`-vs-`"3.0"` whichever `coerce` accepts for the numeric FieldKinds (verify against `coerce::to_field_bytes` for F64→ FieldKind::F64/I64; pick the form coerce round-trips and lock it).

> Determinism note for the implementer: `coerce::to_field_bytes` is the single authority for text→FieldKind bytes. `canonical_f64` must produce a string `coerce` maps to the SAME bytes as the equivalent JSON numeric token would. Add a test pinning `coerce::to_field_bytes(&FieldKind::F64, Cell::Text(canonical_f64(1.5)))` == the IEEE-754 LE bytes of `1.5f64` (and an integer-valued case). If `coerce` is strict about integer FieldKinds rejecting `"7.0"`, render integral physical INT32/INT64 via `I64` (already `to_string()` → `"7"`, fine) and only floats via `canonical_f64`.

- [ ] **Step 4: Run** — `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch --features object-store --test parquet_decode -- --nocapture` → PASS. Default build still excludes it: `cargo test -p kessel-fetch --test parquet_decode 2>&1 | grep "running 0 tests"`. Existing tests unchanged: `cargo test -p kessel-fetch && cargo test -p kessel-fetch --features object-store 2>&1 | grep "test result:"`.

- [ ] **Step 5: Determinism gate** — `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus" | awk '/large_seed/{print} /ok\./{n+=$4} /[1-9][0-9]* failed/{print "REALFAIL"} END{print "TOTAL="n}'` → seed-7 ok, no REALFAIL; record the new total (rose by kessel-parquet's unit tests + the new kessel-fetch test — honest, tracked for T13).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-fetch/Cargo.toml crates/kessel-fetch/src/lib.rs crates/kessel-fetch/src/json.rs crates/kessel-fetch/tests/parquet_decode.rs
git commit -m "parquet: kessel-fetch Format::Parquet arm (PqValue→Cell, reuses coerce path)"
```

---

### Task 9: `do_refresh` + `do_refresh_objstore` format code 3 → Parquet

**Files:** Modify `crates/kesseldb-server/src/router.rs`

- [ ] **Step 1: Add the arm at BOTH match sites** — router.rs has the `match recipe.format { 0=>Json,1=>Csv,2=>Ndjson, n=>SchemaError "unknown format code {n}" }` at ~line 620 (http path) and ~line 998 (`do_refresh_objstore`). In **both**, add `3 => Format::Parquet,` before the `n =>` arm. (`Format::Parquet` exists from Task 8; `kessel_fetch::Format` is already imported in both scopes.)

- [ ] **Step 2: Build matrix** — `cd /c/Users/ihass/KesselDB && cargo build -p kesseldb-server && cargo build -p kesseldb-server --features external-sources && cargo build -p kesseldb-server --features external-sources-tls && cargo build -p kesseldb-server --features external-sources-objstore 2>&1 | tail -2` → all compile. (Format::Parquet under default build: the `#[cfg(not(feature="object-store"))]` arm in rows_from_body returns a typed error — but do_refresh only reaches Parquet via an object-store recipe, and `external-sources-objstore` pulls `object-store`; the default/external-sources/tls builds simply never construct a Parquet recipe. Confirm all four compile.)

- [ ] **Step 3: Regression — existing oracles unchanged** — `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources --test external_source_oracle 2>&1 | grep "test result:" && cargo test -p kesseldb-server --features external-sources-tls --test external_source_tls_oracle 2>&1 | grep "test result:" && cargo test -p kesseldb-server --features external-sources-objstore --test external_source_objstore_oracle 2>&1 | grep "test result:"` → 2 / 1 / 1 unchanged.

- [ ] **Step 4: Gate** — `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus" | awk '/large_seed/{print} /ok\./{n+=$4} /[1-9][0-9]* failed/{print "REALFAIL"} END{print "TOTAL="n}'` → seed-7 ok, no REALFAIL, total unchanged from Task 8 (no new default-build test here).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kesseldb-server/src/router.rs
git commit -m "parquet: do_refresh + do_refresh_objstore map format code 3 to Format::Parquet"
```

---

### Task 10: `kessel-sql` — accept `FORMAT PARQUET` for object stores, reject otherwise

**Files:** Modify `crates/kessel-sql/src/lib.rs`

- [ ] **Step 1: Failing parse tests** — add to kessel-sql tests:

```rust
    #[test]
    fn parquet_accepted_for_object_store() {
        let cat = Catalog::default();
        let op = compile(
            "CREATE EXTERNAL SOURCE p (id U64 NOT NULL FROM 'id') \
             FROM 's3://b/k.parquet' FORMAT PARQUET KEY id \
             REGION 'us-east-1' \
             AUTH OBJSTORE S3 KEYID ENV 'I' SECRET ENV 'S'",
            &cat,
        ).unwrap();
        match op {
            Op::CreateExternalSource { format, url, .. } => {
                assert_eq!(format, 3);
                assert_eq!(url, "s3://b/k.parquet");
            }
            o => panic!("{o:?}"),
        }
        // az:// too
        assert!(compile(
            "CREATE EXTERNAL SOURCE q (id U64 NOT NULL FROM 'id') \
             FROM 'az://c/b.parquet' FORMAT PARQUET KEY id \
             AUTH OBJSTORE AZURE ACCOUNT 'a' KEY ENV 'K'", &cat).is_ok());
    }

    #[test]
    fn parquet_rejected_off_object_store_or_with_page_rows() {
        let cat = Catalog::default();
        let bad = |s: &str| compile(s, &cat).unwrap_err();
        assert!(bad("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
            FROM 'http://h/x.parquet' FORMAT PARQUET KEY id")
            .to_lowercase().contains("object-store"));
        assert!(bad("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
            FROM 'https://h/x.parquet' FORMAT PARQUET KEY id")
            .to_lowercase().contains("object-store"));
        assert!(bad("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
            FROM 's3://b/k' FORMAT PARQUET KEY id REGION 'r' \
            AUTH OBJSTORE S3 KEYID ENV 'I' SECRET ENV 'S' PAGE NEXT LINK")
            .to_lowercase().contains("page"));
        assert!(bad("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
            FROM 's3://b/k' FORMAT PARQUET KEY id REGION 'r' \
            AUTH OBJSTORE S3 KEYID ENV 'I' SECRET ENV 'S' ROWS 'd'")
            .to_lowercase().contains("rows"));
    }
```

- [ ] **Step 2: Run — expect failure** (OBJ-1 currently rejects FORMAT PARQUET entirely).

- [ ] **Step 3: Implement** — find the OBJ-1 PARQUET rejections (`grep -n "PARQUET\|format == 3\|OBJ-2" crates/kessel-sql/src/lib.rs`). Replace the `is_obj` branch's `if format == 3 { return Err("...OBJ-2 not yet shipped...") }` with: *accept* `format == 3` for object-store URLs, but reject `PAGE`/`ROWS` with PARQUET; and in the non-object branch keep/replace the `format == 3` rejection with the object-store-only message. Concretely the validation becomes:

```rust
            if is_obj {
                if format == 3 {
                    if pagination.is_some() {
                        return Err("PAGE clauses are not supported with FORMAT PARQUET".into());
                    }
                    if rows_path.is_some() {
                        return Err("ROWS is not applicable to FORMAT PARQUET".into());
                    }
                    // PARQUET over object store: accepted (OBJ-2a).
                }
                // ... (existing non-PARQUET object-store validation unchanged) ...
            } else {
                if format == 3 {
                    return Err("FORMAT PARQUET is only supported for object-store (s3://|az://) sources".into());
                }
                // ... (existing non-object validation unchanged) ...
            }
```

Match the EXACT structure of the current `is_obj { … } else { … }` block (read it; the OBJ-1 commit `0c747db` shaped it). Keep every other rejection (PARQUET-was-the-only-thing-changing). Ensure `format==3` now flows into the returned `Op::CreateExternalSource { format, .. }` for object stores (it already would — the rejection was the only block).

- [ ] **Step 4: Run** — `cd /c/Users/ihass/KesselDB && cargo test -p kessel-sql -- --nocapture` → all green (the 2 new + every existing CREATE/objstore/pagination test unchanged; the OBJ-1 `objstore_rejections_at_create` test asserted `FORMAT PARQUET` is rejected for object store — UPDATE that one assertion: PARQUET over s3:// is now ACCEPTED; change that specific sub-assertion to expect Ok and adjust its comment, leaving all other rejections in that test intact. Quote the change in your report.)

- [ ] **Step 5: Gate** — `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus" | awk '/large_seed/{print} /ok\./{n+=$4} /[1-9][0-9]* failed/{print "REALFAIL"} END{print "TOTAL="n}'` → seed-7 ok, no REALFAIL.

- [ ] **Step 6: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-sql/src/lib.rs
git commit -m "parquet: SQL accepts FORMAT PARQUET for s3://|az:// (reject http/PAGE/ROWS)"
```

---

### Task 11: Server e2e (feature-gated, fail-closed, SP100 pattern)

**Files:** Create `crates/kesseldb-server/tests/external_source_parquet_oracle.rs`

- [ ] **Step 1: Write the e2e** — copy `crates/kesseldb-server/tests/external_source_objstore_oracle.rs` (the SP100 harness) verbatim; change only: the test fn name (`refresh_parquet_from_s3_fails_closed_and_state_intact`), the env var names, the DDL (`FROM 's3://bucket/data.parquet' FORMAT PARQUET KEY id REGION 'us-east-1' ENDPOINT 'https://127.0.0.1:<port>' AUTH OBJSTORE S3 KEYID ENV ... SECRET ENV ...`), and the stub serves the Parquet fixture bytes (`include_bytes!("../../kessel-parquet/tests/fixtures/flat_required.parquet")`). Header explains: production webpki-roots rejects the self-signed localhost cert ⇒ REFRESH is **fail-closed** (typed `SchemaError`, prior state intact); the trusted Parquet-decode happy-path is proven at the kessel-fetch layer by `parquet_decode.rs` (Task 8) — NO fixture trust injected into the router (SP100 precedent). `#![cfg(feature = "external-sources-objstore")]`. Assert exactly as the SP100 oracle: CREATE `Ok|TypeCreated`; REFRESH → `OpResult::SchemaError(msg)` (msg contains `refresh:`/`sign:`/`tls`/`connect`; panic on any other variant); `SELECT * FROM feed` → `OpResult::Got(b)` with `b.is_empty()`.

- [ ] **Step 2: Run** — `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources-objstore --test external_source_parquet_oracle -- --nocapture` → PASS (fail-closed). Confirm CREATE succeeded (so the failure is genuinely at REFRESH, and proves `FORMAT PARQUET` now parses+routes).

- [ ] **Step 3: Gated out** — `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources --test external_source_parquet_oracle 2>&1 | grep "running 0 tests"`.

- [ ] **Step 4: Gate** — workspace `--release`: seed-7 ok, no REALFAIL, total = Task 8's total (feature-gated test, no default delta).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kesseldb-server/tests/external_source_parquet_oracle.rs
git commit -m "parquet: feature-gated s3:// FORMAT PARQUET e2e (fail-closed, state intact)"
```

---

### Task 12: Pentest pass — harden the attacker-facing Parquet parser

**Files:** Modify `crates/kessel-parquet/src/lib.rs` (test mod) + any real fix found.

- [ ] **Step 1: Adversarial lock tests** — add `#[cfg(test)] mod pentest` in `lib.rs`:

```rust
    #[test]
    fn malformed_parquet_is_typed_error_never_panic() {
        for bad in [
            &b""[..], b"PAR1", b"NOPExxxxxxxxPAR1",
            b"PAR1\xff\xff\xff\xffPAR1",                 // lying meta_len
            b"PAR1\x01\x01\x00\x00\x00PAR1",             // meta_len into header
        ] {
            let r = std::panic::catch_unwind(|| extract(bad, &["id"]));
            assert!(r.is_ok(), "must not panic on {bad:?}");
            assert!(r.unwrap().is_err(), "must be typed err: {bad:?}");
        }
    }

    #[test]
    fn oversized_byte_array_len_and_value_overflow_rejected() {
        // Build a valid footer/metadata for one BYTE_ARRAY column whose
        // page declares a 4GB element length / num_values that would
        // overflow allocation — must return PqError::Bad, not OOM/panic.
        // (Implementer assembles via the Task-4/6 spec-faithful builder,
        //  setting the BYTE_ARRAY length prefix to u32::MAX and a
        //  separate case with dp_num_values = i32::MAX.)
        // assert matches!(extract(&evil_len, &["s"]), Err(PqError::Bad(_)));
        // assert matches!(extract(&evil_cnt, &["s"]), Err(PqError::Bad(_)));
    }
```

Implement the second test's `evil_len`/`evil_cnt` with the spec-faithful builder. Run `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet pentest -- --nocapture`.

- [ ] **Step 2: Fix any real issue.** Audit `extract`/`plain.rs`/`meta.rs`/`footer.rs`/`thrift.rs` for: any `Vec::with_capacity(n)` where `n` is file-controlled (use a sane cap or `try_reserve`/incremental push — `decode_plain`'s `Vec::with_capacity(count)` with attacker `count` is the prime suspect; cap `count` against the remaining page bytes / a hard ceiling before allocating, or push without pre-reserve); any multiply/add on file offsets not already `checked_`; any slice index not via `.get()`. The `#![forbid(unsafe_code)]` already removes UB; the goal is "typed error, never panic/OOM" on hostile input. Apply minimal fixes; the lock tests must pass.

- [ ] **Step 3: Gate + commit** — `cargo test -p kessel-parquet` green; workspace `--release` seed-7 ok / no REALFAIL.

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-parquet
git commit -m "parquet: pentest hardening — bounded allocation + malformed-input lock tests"
```
(If no code fix was needed, commit message: `parquet: lock malformed-input/no-panic invariants (pentest pass)`.)

---

### Task 13: Docs + gate reconciliation + internal record

**Files:** Modify `docs/USAGE.md`, `docs/STATUS.md`, `README.md`; Create `docs/superpowers/specs/2026-05-19-kesseldb-subproject101-parquet.md`

- [ ] **Step 1: Measure** — `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus" | awk '/large_seed/{print} /ok\./{n+=$4} /[1-9][0-9]* failed/{print "REALFAIL"} END{print "TOTAL="n}'` → record `PQ_TOTAL`, seed-7 ok, no REALFAIL. `cargo tree -p kesseldb-server -e normal | grep -iE "rustls|webpki|objstore|parquet" || echo "DEFAULT CLEAN"` → `DEFAULT CLEAN`.

- [ ] **Step 2: USAGE** — in `docs/USAGE.md` §7 object-store subsection, add `FORMAT PARQUET` (object-store only; PLAIN/uncompressed/flat-REQUIRED/V1 in OBJ-2a; `ColumnMap.source` = the flat leaf column name; rejected over http(s):// and with PAGE/ROWS; dictionary/compression/OPTIONAL/nested/V2/INT96 are OBJ-2b/2c and rejected with a clear error). Precise, no overclaim.

- [ ] **Step 3: STATUS + README** — STATUS SP101 line (Parquet object sources OBJ-2a shipped: pure-Rust zero-dep kessel-parquet, PLAIN/uncompressed/flat-REQUIRED/V1/multi-RG/mapped-subset; honest gate 267→PQ_TOTAL because kessel-parquet is a workspace member + kessel-fetch parquet test — NOT zero-delta; kernel zero-dep, default build links no parquet/objstore/rustls; seed-7 green). README headline test count → PQ_TOTAL (read current, set measured). README "Honest boundaries": note FORMAT PARQUET requires `--features external-sources-objstore` and is OBJ-2a-limited.

- [ ] **Step 4: Internal record** — `docs/superpowers/specs/2026-05-19-kesseldb-subproject101-parquet.md`: design link; builds on SP97-100; what shipped (kessel-parquet thrift/footer/meta/plain/lib; Format::Parquet + pq_to_cell reusing coerce; do_refresh×2 + sql flip; e2e); the KAT/fixture provenance (real writer vs spec-faithful generator — state which; the spec-published-byte primitive KATs are the independent authority); honest gate accounting 267→PQ_TOTAL + the real reason (workspace-member crate + parquet test) + invariants that hold; security/pentest findings; determinism boundary (Parquet decode pure on captured bytes; pq_to_cell reuses the coerce path so FieldKind bytes are source-format-independent); deferred OBJ-2b (dictionary/RLE + Snappy + OPTIONAL/def-levels) / OBJ-2c (gzip/zstd + INT96/DECIMAL + nested-skip + V2 pages) + carried OBJ/EXT deferrals.

- [ ] **Step 5: Final gate + commit**

Run the Step-1 measure again → unchanged.

```bash
cd /c/Users/ihass/KesselDB
git add docs/ README.md
git commit -m "docs: Parquet object sources OBJ-2a — USAGE/STATUS/README + subproject101 record"
git push
```

(Auto-memory is updated by the controller after the final review, not here.)

---

## Self-Review

**1. Spec coverage:** §0 decompose/2a-scope → Task 0 + the per-task OBJ-2a-only gates; §1 architecture (kessel-parquet crate, PqValue→Cell reuse, no cycle, do_refresh×2, whole-object decode) → Tasks 1,6,8,9; §2 file split (thrift/footer/meta/plain/lib) → Tasks 1–6; §3 support-matrix fail-closed errors → Task 6 + Task 12; §4 SQL flip + 3 rejections → Task 10; §5 determinism/security/back-compat (reuse coerce, no kernel/proto/catalog change, existing oracles green, feature-off not compiled) → Tasks 8,9,11 + every gate; §6 testing (spec-byte KATs, real/spec-faithful fixture, feature-on decode, sql parse, fail-closed e2e, honest gate) → Tasks 2–13; §7 non-goals → Task 6 gate + Task 10 + Task 13 docs. No gap.

**2. Placeholder scan:** All code steps contain complete code. Two **deliberate, sourced** instruction-steps (Task 7 real-writer-first/BLOCK-not-fake; Task 12 Step-2 audit) carry the exact discipline + the precise suspects + a BLOCK fallback — not silent TODOs; they exist because faking a Parquet fixture or hand-waving the hardening would be worse, mirroring OBJ-1's accepted KAT/pentest pattern. Task 6/12 "assemble via the spec-faithful builder" references the concrete `uv`/`zz`/metadata builder defined verbatim in Task 4.

**3. Type consistency:** `PqValue{Null,Bool(bool),I64(i64),F64(f64),Bytes(Vec<u8>)}` / `PqError{Bad,Unsupported}` / `pub fn extract(&[u8],&[&str])->Result<Vec<Vec<PqValue>>,PqError>` consistent Tasks 1,6,7,8. `thrift::{Reader,StructReader,Field,ctype,zigzag_decode}` consistent Tasks 2,4. `meta::{FileMetaData,SchemaLeaf,ColumnChunk,RowGroup,Type,Repetition,Codec,Encoding,PageHeader,decode_page_header}` consistent Tasks 4,6. `plain::decode_plain(&[u8],Type,usize)` consistent Tasks 5,6. `Format::Parquet` + `pq_to_cell`+`json::canonical_f64` consistent Tasks 8,9. `reader_pos`/`Reader::pos` added in Task 4 and used by `decode_page_header` — consistent. Gate accounting honest (kessel-parquet workspace-member tests rise the total; Task 13 reconciles — no false zero-delta, matching SP100's corrected stance).
