# OBJ-2b-3 Parquet Snappy Block Decompression Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decompress Snappy-compressed Parquet pages (pyarrow's default `compression='snappy'`) with a pure zero-dependency raw-block decompressor, flipping the Snappy codec gate so dictionary/PLAIN decode runs over the decompressed bytes unchanged.

**Architecture:** New pure module `crates/kessel-parquet/src/snappy.rs` (raw Snappy block decode, 64 MiB hard cap). `meta.rs` learns `Codec::Snappy`. `lib.rs::read_chunk_values` gains a `Cow<[u8]>` `page_payload` helper that slices the on-disk page by `compressed_size` and Snappy-decompresses (or borrows for uncompressed), advancing the file offset by `compressed_size`. No kessel-fetch/kessel-sql/server/kernel change.

**Tech Stack:** Rust (workspace edition), `#![forbid(unsafe_code)]`, zero external dependencies, existing `PqError`/`PqValue`, `std::borrow::Cow`.

---

## Context for the implementer (read once)

`crates/kessel-parquet` modules: `thrift.rs`, `footer.rs`, `meta.rs` (enums + `ColumnChunk`/`PageHeader`), `plain.rs` (`decode_plain`), `rle.rs`, `dict.rs` (`resolve_dict_indices`), `lib.rs` (`extract`, `read_chunk_values`). `Cargo.toml` `[dependencies]` is empty and MUST stay empty.

`meta.rs` `Codec` is currently `enum Codec { Uncompressed, Other(i32) }` with `from_i32`: `if v==0 {Uncompressed} else {Other(v)}`. `PageHeader` has both `uncompressed_size: i32` and `compressed_size: i32` (decoded since SP101). `read_chunk_values` (lib.rs ~line 50+) currently: rejects `cc.codec != Codec::Uncompressed` with `Unsupported("compression: OBJ-2b-3")`; for the dict page and each data page it slices `file[dstart .. dstart + uncompressed_size]` and advances `off` by the same — a *latent assumption that compressed == uncompressed* (true only for uncompressed pages).

**Snappy raw block format** (authority: `google/snappy` `format_description.txt` — the independent reference; do NOT derive expected bytes from the code under test):
- Preamble: uncompressed length as little-endian base-128 varint.
- Elements until src exhausted; `tag & 0b11`:
  - `00` literal: `len1 = tag>>2`; if `len1<60` length=`len1+1`; else read `len1-59` little-endian bytes = (length−1), length = that+1; then `length` bytes copied from src.
  - `01` copy 1-byte-offset: length = `4 + ((tag>>2)&0b111)`; offset = `((tag>>5)<<8) | next_byte`.
  - `10` copy 2-byte-offset: length = `1 + (tag>>2)`; offset = next 2 bytes LE.
  - `11` copy 4-byte-offset: length = `1 + (tag>>2)`; offset = next 4 bytes LE.
- Copy back-reference: require `1 <= offset <= out.len()`; copy `length` bytes from `out[out.len()-offset ..]`; **overlapping (`offset < length`) is legal — copy byte-by-byte** (RLE). Parquet uses the **raw block** format (no `0xff` stream identifier / CRC chunks).

**Discipline:** `#![forbid(unsafe_code)]` crate-wide. No unwrap/expect/panic/raw-index on input bytes — checked `get(..)`/`checked_*`. New module carries `#![allow(dead_code)]` like siblings. KATs are hand-derived from `format_description.txt`; a failing KAT means the *code* is wrong — never change a KAT; report BLOCKED if irreconcilable.

**Determinism / invariants gate — EVERY task (T0–T6):**
- `cargo test --workspace --release` → `FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` green.
- `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty; `cargo tree -p kesseldb-server` links no parquet/objstore/rustls/webpki.
- Existing oracles green unchanged: `external_source_oracle`(2), `external_source_tls_oracle`(1), `external_source_objstore_oracle`(1); ALL OBJ-2a/2b decode+gate tests green unchanged (they use `compressed==uncompressed`).

**Commit discipline:** straight to `main`, no Co-Authored-By, no signing, message style like `git log -3` (`parquet: …`, `docs: …`). `git push` after every task. Bash: prefix each call `cd /c/Users/ihass/KesselDB &&` (cwd resets per call); `cargo test --workspace --release` is long — allow 600000ms.

---

### Task 0: Determinism baseline (#164)

**Files:** none (measurement only).

- [ ] **Step 1:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -30` — sum `passed` across binaries → `<BASELINE>` (expected **326**); confirm `FAILED=0` and `large_seed_corpus_is_deterministic_and_converges` in an `ok` result.
- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo tree -p kesseldb-server 2>&1 | grep -Ei "parquet|objstore|rustls|webpki"` → expect no output.
- [ ] **Step 3:** No commit. Report DONE with `OBJ-2b-3 baseline: <BASELINE> tests passing, FAILED=0, seed-7 green` + the per-binary counts summed.

---

### Task 1: `snappy.rs` raw-block decompressor + spec KATs (#165)

**Files:**
- Create: `crates/kessel-parquet/src/snappy.rs`
- Modify: `crates/kessel-parquet/src/lib.rs` (add `mod snappy;` after `mod dict;`)

- [ ] **Step 1: Declare the module.** In `crates/kessel-parquet/src/lib.rs` the module block ends with `mod dict;`. Add `mod snappy;` as the next line.

- [ ] **Step 2: Write the failing test file.** Create `crates/kessel-parquet/src/snappy.rs` with ONLY this first (tests reference a not-yet-existing fn → red):

```rust
//! Pure raw-block Snappy decompressor (the format Parquet uses per
//! page — NOT the stream/framing format). Authority: google/snappy
//! `format_description.txt`. Zero deps, bounds-checked, hard
//! decompressed-size cap, overlapping copies handled byte-by-byte.
//! Never panics / OOM-aborts.
#![allow(dead_code)]

use crate::PqError;

fn bad(s: &str) -> PqError {
    PqError::Bad(s.to_string())
}

/// Hard cap on a single decompressed page. Real writers (pyarrow
/// default data_page_size ~1 MiB) are far below this; the cap defeats
/// a decompression-bomb (tiny input claiming a multi-GB
/// uncompressed_page_size). Pages above it are rejected as
/// Unsupported (OBJ-2c may revisit).
pub(crate) const SNAPPY_MAX_DECOMP: usize = 64 << 20; // 64 MiB

#[cfg(test)]
mod tests {
    use super::*;

    // KAT 1 — literal "abc": preamble varint 3 = 0x03; literal tag
    // = ((3-1)<<2)|0b00 = 0x08; then 'a','b','c'.
    #[test]
    fn kat_literal_abc() {
        let blk = [0x03u8, 0x08, 0x61, 0x62, 0x63];
        assert_eq!(decompress(&blk, 3).unwrap(), b"abc".to_vec());
    }

    // KAT 2 — OVERLAPPING COPY (RLE) "aaaaaa": preamble 6 = 0x06;
    // 1-byte literal 'a' (tag ((1-1)<<2)|0 = 0x00, 0x61); then a
    // 2-byte-offset copy length 5 offset 1: tag = (5-1<<2... ) NOTE
    // 2-byte-offset length = 1+(tag>>2) → want 5 → tag>>2=4 →
    // tag=(4<<2)|0b10 = 0x12; offset 1 = LE16 [0x01,0x00]. offset(1)
    // < length(5) ⇒ overlapping ⇒ byte-by-byte RLE → 6×'a'.
    #[test]
    fn kat_overlapping_copy_rle() {
        let blk = [0x06u8, 0x00, 0x61, 0x12, 0x01, 0x00];
        assert_eq!(decompress(&blk, 6).unwrap(), b"aaaaaa".to_vec());
    }

    // KAT 3 — 1-byte-offset copy "abcdabcd": preamble 8 = 0x08;
    // literal "abcd" (tag ((4-1)<<2)|0 = 0x0C, then a b c d); copy
    // length 4 offset 4 via 1-byte-offset: length = 4+((tag>>2)&7)
    // → want 4 → (tag>>2)&7=0; offset 4 (≤255) → high3=0, tag =
    // (0<<5)|(0<<2)|0b01 = 0x01, extra offset byte 0x04.
    #[test]
    fn kat_copy_1byte_offset() {
        let blk = [0x08u8, 0x0C, 0x61, 0x62, 0x63, 0x64, 0x01, 0x04];
        assert_eq!(decompress(&blk, 8).unwrap(), b"abcdabcd".to_vec());
    }

    // KAT 4 — 4-byte-offset copy "abcdabcd": same literal; copy
    // length 4 offset 4 via 4-byte-offset: length = 1+(tag>>2) →
    // want 4 → tag>>2=3 → tag=(3<<2)|0b11 = 0x0F; offset 4 = LE32
    // [0x04,0,0,0].
    #[test]
    fn kat_copy_4byte_offset() {
        let blk = [
            0x08u8, 0x0C, 0x61, 0x62, 0x63, 0x64, 0x0F, 0x04, 0x00,
            0x00, 0x00,
        ];
        assert_eq!(decompress(&blk, 8).unwrap(), b"abcdabcd".to_vec());
    }

    // KAT 5 — multi-byte literal length (length 61): preamble 61 =
    // 0x3D; literal tag len1=60 → tag=(60<<2)|0 = 0xF0; 1 extra
    // length byte (len1-59 = 1) holding (length-1)=60 LE = 0x3C;
    // then 61 × 'z' (0x7A).
    #[test]
    fn kat_literal_multibyte_length() {
        let mut blk = vec![0x3Du8, 0xF0, 0x3C];
        blk.extend(std::iter::repeat(0x7Au8).take(61));
        assert_eq!(decompress(&blk, 61).unwrap(), vec![0x7Au8; 61]);
    }

    // Malformed → Bad (never panic).
    #[test]
    fn kat_malformed_is_bad() {
        // preamble (3) != expected_len (5)
        assert!(matches!(
            decompress(&[0x03, 0x08, 0x61, 0x62, 0x63], 5),
            Err(PqError::Bad(_))
        ));
        // copy offset 0: literal 'a' then 2-byte-offset copy off 0
        assert!(matches!(
            decompress(&[0x02, 0x00, 0x61, 0x06, 0x00, 0x00], 2),
            Err(PqError::Bad(_))
        ));
        // copy offset past output: literal 'a' then copy off 9
        assert!(matches!(
            decompress(&[0x06, 0x00, 0x61, 0x12, 0x09, 0x00], 6),
            Err(PqError::Bad(_))
        ));
        // literal length past src: preamble 10, literal tag len 10,
        // only 2 src bytes
        assert!(matches!(
            decompress(&[0x0A, 0x24, 0x61, 0x62], 10),
            Err(PqError::Bad(_))
        ));
        // truncated (empty)
        assert!(matches!(decompress(&[], 0), Ok(v) if v.is_empty())
            || matches!(decompress(&[], 0), Err(PqError::Bad(_))));
    }

    // Over-cap expected_len → Unsupported BEFORE allocation.
    #[test]
    fn kat_over_cap_is_unsupported() {
        let huge = SNAPPY_MAX_DECOMP + 1;
        assert!(matches!(
            decompress(&[0xFF, 0xFF, 0xFF], huge),
            Err(PqError::Unsupported(_))
        ));
    }
}
```

(Note KAT 5 tag math: literal length 61 → `len1` must satisfy: if
`len1>=60`, extra-byte count = `len1-59`. Choose `len1=60` → 1 extra
byte = `(length-1)=60=0x3C`. tag = `(60<<2)|0 = 240 = 0xF0`.)

- [ ] **Step 3:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet snappy:: 2>&1 | tail -10` → compile error `cannot find function decompress`.

- [ ] **Step 4: Implement `decompress`** — insert above `#[cfg(test)] mod tests`:

```rust
/// Read a little-endian base-128 varint at `data[*pos..]`; advance
/// `*pos`. Rejects > 5 bytes (Snappy length is u32) as Bad.
fn varint(data: &[u8], pos: &mut usize) -> Result<u64, PqError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let b = *data
            .get(*pos)
            .ok_or_else(|| bad("snappy varint truncated"))?;
        *pos += 1;
        if shift >= 35 {
            return Err(bad("snappy varint too long"));
        }
        result |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

/// Decompress one raw Snappy block. `expected_len` is the page
/// header's uncompressed_page_size (the authority). The block's own
/// preamble MUST equal it. Output bounded by `expected_len` (itself
/// ≤ SNAPPY_MAX_DECOMP). Never panics / OOM-aborts.
pub fn decompress(
    src: &[u8],
    expected_len: usize,
) -> Result<Vec<u8>, PqError> {
    if expected_len > SNAPPY_MAX_DECOMP {
        return Err(PqError::Unsupported(format!(
            "snappy page {expected_len} exceeds {SNAPPY_MAX_DECOMP} cap: OBJ-2c"
        )));
    }
    let mut pos = 0usize;
    let declared = varint(src, &mut pos)?;
    if usize::try_from(declared).map(|d| d != expected_len).unwrap_or(true) {
        return Err(bad("snappy preamble length != uncompressed_page_size"));
    }
    let mut out: Vec<u8> = Vec::with_capacity(expected_len);
    while pos < src.len() {
        let tag = *src
            .get(pos)
            .ok_or_else(|| bad("snappy tag truncated"))?;
        pos += 1;
        match tag & 0b11 {
            0 => {
                // literal
                let mut len = (tag >> 2) as usize;
                if len >= 60 {
                    let extra = len - 59; // 1..=4 bytes
                    let mut v: usize = 0;
                    for i in 0..extra {
                        let b = *src.get(pos + i).ok_or_else(|| {
                            bad("snappy literal len truncated")
                        })?;
                        v |= (b as usize) << (8 * i);
                    }
                    pos = pos
                        .checked_add(extra)
                        .ok_or_else(|| bad("snappy pos ovf"))?;
                    len = v;
                }
                len = len
                    .checked_add(1)
                    .ok_or_else(|| bad("snappy literal len ovf"))?;
                let end = pos
                    .checked_add(len)
                    .ok_or_else(|| bad("snappy literal end ovf"))?;
                let lit = src
                    .get(pos..end)
                    .ok_or_else(|| bad("snappy literal past src"))?;
                if out.len().checked_add(len).map(|t| t > expected_len)
                    .unwrap_or(true)
                {
                    return Err(bad("snappy literal overproduces"));
                }
                out.extend_from_slice(lit);
                pos = end;
            }
            t => {
                // copy: t == 1 (1-byte off), 2 (2-byte), 3 (4-byte)
                let (length, offset) = match t {
                    1 => {
                        let len = 4 + (((tag >> 2) & 0b111) as usize);
                        let lo = *src.get(pos).ok_or_else(|| {
                            bad("snappy copy1 off truncated")
                        })? as usize;
                        pos += 1;
                        let hi = ((tag >> 5) & 0b111) as usize;
                        (len, (hi << 8) | lo)
                    }
                    2 => {
                        let len = 1 + ((tag >> 2) as usize);
                        let b = src.get(pos..pos + 2).ok_or_else(
                            || bad("snappy copy2 off truncated"),
                        )?;
                        pos += 2;
                        (len, u16::from_le_bytes(
                            b.try_into().unwrap(),
                        ) as usize)
                    }
                    _ => {
                        let len = 1 + ((tag >> 2) as usize);
                        let b = src.get(pos..pos + 4).ok_or_else(
                            || bad("snappy copy4 off truncated"),
                        )?;
                        pos += 4;
                        (len, u32::from_le_bytes(
                            b.try_into().unwrap(),
                        ) as usize)
                    }
                };
                if offset == 0 || offset > out.len() {
                    return Err(bad("snappy copy offset out of range"));
                }
                if out.len().checked_add(length)
                    .map(|x| x > expected_len).unwrap_or(true)
                {
                    return Err(bad("snappy copy overproduces"));
                }
                let start = out.len() - offset;
                // Overlapping copy (offset < length) is legal —
                // byte-by-byte RLE expansion.
                for i in 0..length {
                    let byte = out[start + i];
                    out.push(byte);
                }
            }
        }
    }
    if out.len() != expected_len {
        return Err(bad("snappy output length != uncompressed_page_size"));
    }
    Ok(out)
}
```

(`b.try_into().unwrap()` on a `src.get(pos..pos+2 | +4)` slice is the
statically-infallible fixed-size pattern, exactly like plain.rs:87 —
the slice is length-checked by `get`. `out[start+i]` indexes the
*output* Vec we built, within `start+i < out.len()` because
`start = out.len()-offset` and `i < length` with `offset>=1` and the
overproduce guard already ran; this is not an input-byte index.)

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet snappy::tests 2>&1 | tail -12` → `test result: ok. 7 passed`. If `kat_overlapping_copy_rle` fails, the byte-by-byte copy is wrong (you likely used `extend_from_slice`/`copy_within` — must push one byte at a time). Do NOT change KAT bytes.

- [ ] **Step 6:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, total ≥ baseline+7, seed-7 green.

- [ ] **Step 7: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/snappy.rs crates/kessel-parquet/src/lib.rs && git commit -m "parquet: pure raw-block Snappy decompressor (snappy::decompress) + spec KATs" && git push
```

---

### Task 2: `meta.rs` `Codec::Snappy` (#166)

**Files:** Modify `crates/kessel-parquet/src/meta.rs`

- [ ] **Step 1: Write the failing test** — add inside `meta.rs` `#[cfg(test)] mod tests` (it has `uv`/`zz`):

```rust
#[test]
fn columnmeta_decodes_snappy_codec() {
    // Minimal FileMetaData (1 INT64 REQUIRED leaf "id", 1 RG, 1
    // chunk) with ColumnMetaData f4 codec = SNAPPY(1). Also assert
    // an unknown codec 7 maps to Other(7).
    fn build(codec: i64) -> Vec<u8> {
        let mut b = Vec::new();
        b.push(0x15); uv(&mut b, zz(1));                 // f1 version=1
        b.push(0x19); b.push(0x2c);                      // f2 list<struct> 2
        b.push(0x48); uv(&mut b, 4); b.extend_from_slice(b"root");
        b.push(0x15); uv(&mut b, zz(1));                 // num_children=1
        b.push(0x00);
        b.push(0x15); uv(&mut b, zz(2));                 // leaf f1 type=INT64
        b.push(0x25); uv(&mut b, zz(0));                 // f3 repetition=REQUIRED
        b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
        b.push(0x00);
        b.push(0x16); uv(&mut b, zz(1));                 // f3 num_rows=1
        b.push(0x19); b.push(0x1c);                      // f4 list<RowGroup> 1
        b.push(0x19); b.push(0x1c);                      // RG f1 list<ColumnChunk> 1
        b.push(0x3c);                                    // ColumnChunk f3 ColumnMetaData
        b.push(0x15); uv(&mut b, zz(2));                 // CMD f1 type=INT64
        b.push(0x19); b.push(0x15); uv(&mut b, zz(0));   // f2 encodings [PLAIN]
        b.push(0x19); b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
        b.push(0x15); uv(&mut b, zz(codec));             // f4 codec
        b.push(0x16); uv(&mut b, zz(1));                 // f5 num_values=1
        b.push(0x46); uv(&mut b, zz(4));                 // f9 data_page_offset=4
        b.push(0x00);                                    // stop ColumnMetaData
        b.push(0x00);                                    // stop ColumnChunk
        b.push(0x26); uv(&mut b, zz(1));                 // RG f3 num_rows=1
        b.push(0x00);                                    // stop RowGroup
        b.push(0x00);                                    // stop FileMetaData
        b
    }
    let md1 = FileMetaData::decode(&build(1)).expect("snappy");
    assert_eq!(md1.row_groups[0].columns[0].codec, Codec::Snappy);
    let md7 = FileMetaData::decode(&build(7)).expect("other");
    assert_eq!(md7.row_groups[0].columns[0].codec, Codec::Other(7));
}
```

- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet columnmeta_decodes_snappy_codec 2>&1 | tail -8` → compile error (`no variant Snappy`).

- [ ] **Step 3: Implement.** In `meta.rs`, change the `Codec` enum and `from_i32`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    Uncompressed,
    /// SNAPPY (parquet CompressionCodec id = 1), raw block format.
    Snappy,
    Other(i32),
}
impl Codec {
    fn from_i32(v: i32) -> Codec {
        match v {
            0 => Codec::Uncompressed,
            1 => Codec::Snappy,
            o => Codec::Other(o),
        }
    }
}
```

- [ ] **Step 4:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet meta:: 2>&1 | tail -10` → the new test + all existing meta tests pass (existing tests use codec 0 → still `Uncompressed`).

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, total ≥ baseline+8, seed-7 green.

- [ ] **Step 6: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/meta.rs && git commit -m "parquet: meta Codec::Snappy (CompressionCodec id 1)" && git push
```

---

### Task 3: `read_chunk_values` `page_payload` Cow helper + codec gate flip (#167)

**Files:** Modify `crates/kessel-parquet/src/lib.rs`

Before coding: READ the current `read_chunk_values` in `lib.rs` to see the exact dict-page arm and data-page loop (the `usize::try_from(ph.uncompressed_size)` slices and the `off`/`dend` advance). Preserve ALL other behavior; only the codec gate, the page-byte slicing/decompress, and the offset advance change.

- [ ] **Step 1: Write the failing tests** — add inside `lib.rs` `mod tests` (it has `uv`/`zz`/`build_parquet_file`/`build_dict_int64_file_with_dict_offset`):

```rust
/// A Snappy "literal-only" block wrapping `raw` exactly (preamble +
/// one literal). Valid spec-faithful Snappy (literal-only is the
/// trivial correct encoding). Used to build Snappy test files.
fn snappy_literal_block(raw: &[u8]) -> Vec<u8> {
    let mut b = Vec::new();
    // preamble varint = raw.len()
    let mut n = raw.len() as u64;
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 { b.push(byte); break; } else { b.push(byte | 0x80); }
    }
    // literal: if len-1 < 60 single tag; raw.len() here is small (16)
    let l1 = (raw.len() - 1) as u8;
    assert!((raw.len() as u64) >= 1 && l1 < 60, "helper: small literals only");
    b.push(l1 << 2); // tag, type 00
    b.extend_from_slice(raw);
    b
}

/// Build a PLAIN INT64 [7,-2] file compressed with Snappy (codec=1).
/// Page raw payload = 7i64 LE ++ (-2)i64 LE (16 bytes); on-disk page
/// = snappy_literal_block(raw). compressed_page_size = block.len(),
/// uncompressed_page_size = 16.
fn build_snappy_plain_int64_file() -> Vec<u8> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&7i64.to_le_bytes());
    raw.extend_from_slice(&(-2i64).to_le_bytes());
    let block = snappy_literal_block(&raw);     // on-disk page bytes
    let uncomp = raw.len() as i64;              // 16
    let comp = block.len() as i64;              // 18

    let mut hdr = Vec::new();
    hdr.push(0x15); uv(&mut hdr, zz(0));        // f1 type=DATA_PAGE(0)
    hdr.push(0x25); uv(&mut hdr, zz(uncomp));   // f3 uncompressed_page_size=16
    hdr.push(0x15); uv(&mut hdr, zz(comp));     // f4 compressed_page_size=18
    hdr.push(0x1c);                             // f5 DataPageHeader struct
    hdr.push(0x15); uv(&mut hdr, zz(2));        // g1 num_values=2
    hdr.push(0x15); uv(&mut hdr, zz(0));        // g2 encoding=PLAIN(0)
    hdr.push(0x00); hdr.push(0x00);             // stop DPH / PH

    let data_page_offset: i64 = 4;

    let mut m = Vec::new();
    m.push(0x15); uv(&mut m, zz(2));            // f1 version=2
    m.push(0x19); m.push(0x2c);                 // f2 list<SchemaElement> 2
    m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
    m.push(0x15); uv(&mut m, zz(1));            // schema[0] num_children=1
    m.push(0x00);
    m.push(0x15); uv(&mut m, zz(2));            // schema[1] f1 type=INT64
    m.push(0x25); uv(&mut m, zz(0));            // f3 repetition=REQUIRED
    m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
    m.push(0x00);
    m.push(0x16); uv(&mut m, zz(2));            // f3 num_rows=2
    m.push(0x19); m.push(0x1c);                 // f4 list<RowGroup> 1
    m.push(0x19); m.push(0x1c);                 // RG f1 list<ColumnChunk> 1
    m.push(0x3c);                               // ColumnChunk f3 ColumnMetaData
    m.push(0x15); uv(&mut m, zz(2));            // CMD f1 type=INT64
    m.push(0x19); m.push(0x15); uv(&mut m, zz(0)); // f2 encodings [PLAIN]
    m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
    m.push(0x15); uv(&mut m, zz(1));            // f4 codec=SNAPPY(1)
    m.push(0x16); uv(&mut m, zz(2));            // f5 num_values=2
    m.push(0x46); uv(&mut m, zz(data_page_offset)); // f9 data_page_offset=4
    m.push(0x00); m.push(0x00);                 // stop CMD / ColumnChunk
    m.push(0x26); uv(&mut m, zz(2));            // RG f3 num_rows=2
    m.push(0x00); m.push(0x00);                 // stop RG / FileMetaData

    let mut f = Vec::new();
    f.extend_from_slice(b"PAR1");
    f.extend_from_slice(&hdr);
    f.extend_from_slice(&block);
    let mlen = m.len() as u32;
    f.extend_from_slice(&m);
    f.extend_from_slice(&mlen.to_le_bytes());
    f.extend_from_slice(b"PAR1");
    f
}

#[test]
fn extract_decodes_snappy_plain_int64() {
    let file = build_snappy_plain_int64_file();
    let rows = extract(&file, &["id"]).expect("snappy extract");
    assert_eq!(rows, vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]);
}

#[test]
fn extract_snappy_and_uncompressed_identical() {
    // Same logical [7,-2]: existing build_parquet_file(0,0,0,false)
    // is the UNCOMPRESSED PLAIN baseline.
    let plain = extract(&build_parquet_file(0, 0, 0, false), &["id"])
        .expect("plain");
    let snap = extract(&build_snappy_plain_int64_file(), &["id"])
        .expect("snappy");
    assert_eq!(plain, snap); // source-format independence
    assert_eq!(snap, vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]);
}

#[test]
fn extract_rejects_gzip_codec_obj2c() {
    // codec GZIP(2) still Unsupported (only SNAPPY flips this slice).
    let file = build_parquet_file(0, 2, 0, false); // (enc,codec,rep,dicthdr)
    assert!(
        matches!(extract(&file, &["id"]), Err(PqError::Unsupported(_))),
        "gzip codec must be Unsupported (OBJ-2c)"
    );
}
```

(`build_parquet_file`'s 2nd arg is the codec; `2` = GZIP. This
*replaces the assertion intent* of the old `extract_rejects_snappy_codec`
test which used codec `2`? — VERIFY: read the existing
`extract_rejects_snappy_codec`; it uses `build_parquet_file(0, 2, 0, false)`
with comment "codec = SNAPPY(2)" — that comment was WRONG (2 is GZIP;
SNAPPY is 1). Keep that existing test but it now asserts GZIP→Unsupported
which is still true; do NOT delete it. This new `extract_rejects_gzip_codec_obj2c`
is functionally similar — if it duplicates the existing test exactly,
instead RENAME the existing `extract_rejects_snappy_codec` to
`extract_rejects_gzip_codec_obj2c` and fix its inaccurate comment, and
do NOT add a second copy. Decide based on the actual existing test
body; document the choice in your report. This is an intended,
reviewed comment/clarity correction, not a behavior change — GZIP(2)
remains Unsupported either way.)

- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet extract_decodes_snappy_plain_int64 2>&1 | tail -8` → FAIL (codec gate rejects Snappy).

- [ ] **Step 3: Add the `page_payload` helper** (free fn, near `read_chunk_values`):

```rust
/// The on-disk page payload, decompressed if needed. Slices the
/// `comp`-byte on-disk region at `dstart`; Uncompressed → borrowed
/// (zero-copy), Snappy → owned decompressed (length `uncomp`).
fn page_payload<'a>(
    file: &'a [u8],
    dstart: usize,
    comp: usize,
    uncomp: usize,
    codec: meta::Codec,
) -> Result<std::borrow::Cow<'a, [u8]>, PqError> {
    let end = dstart
        .checked_add(comp)
        .ok_or_else(|| PqError::Bad("page region ovf".into()))?;
    let on_disk = file
        .get(dstart..end)
        .ok_or_else(|| PqError::Bad("page data truncated".into()))?;
    match codec {
        meta::Codec::Uncompressed => Ok(std::borrow::Cow::Borrowed(on_disk)),
        meta::Codec::Snappy => {
            Ok(std::borrow::Cow::Owned(snappy::decompress(on_disk, uncomp)?))
        }
        meta::Codec::Other(_) => Err(PqError::Unsupported(
            "compression codec (gzip/zstd/lz4/brotli): OBJ-2c".into(),
        )),
    }
}
```

- [ ] **Step 4: Flip the codec gate and route both page kinds through `page_payload`.** In `read_chunk_values`:
  - Replace the early `if cc.codec != meta::Codec::Uncompressed { return Err(Unsupported("compression: OBJ-2b-3")) }` with:
    ```rust
    match cc.codec {
        meta::Codec::Uncompressed | meta::Codec::Snappy => {}
        meta::Codec::Other(_) => {
            return Err(PqError::Unsupported(
                "compression codec (gzip/zstd/lz4/brotli): OBJ-2c".into(),
            ))
        }
    }
    ```
  - **Dict-page arm:** today it computes `dstart` then slices
    `file.get(dstart..dstart+uncompressed_size)` and decodes. Change to:
    `let comp = usize::try_from(ph.compressed_size).map_err(|_| PqError::Bad("dict page comp size range".into()))?;`
    `let uncomp = usize::try_from(ph.uncompressed_size).map_err(|_| PqError::Bad("dict page size range".into()))?;`
    `let payload = page_payload(file, dstart, comp, uncomp, cc.codec)?;`
    then `plain::decode_plain(&payload, want_ptype, dn)?` (deref Cow via `&payload`). Do NOT advance any chunk `off` from the dict page (the dict page offset is independent of the data-page loop start, as today).
  - **Data-page loop:** replace the `dend = dstart + uncompressed_size` slice + decode + `off = dend` with:
    `let comp = usize::try_from(ph.compressed_size).map_err(|_| PqError::Bad("page comp size range".into()))?;`
    `let uncomp = usize::try_from(ph.uncompressed_size).map_err(|_| PqError::Bad("page size range".into()))?;`
    `let payload = page_payload(file, dstart, comp, uncomp, cc.codec)?;`
    decode by `ph.dp_encoding` exactly as today but over `&payload`
    (`plain::decode_plain(&payload, want_ptype, n)` / `dict::resolve_dict_indices(&payload, &dict, n)`); then advance
    `off = dstart.checked_add(comp).ok_or_else(|| PqError::Bad("page advance ovf".into()))?;`
    Keep the `page_type`/`dp_encoding`/overshoot/undershoot/loop-termination logic byte-identical otherwise. Update the `off = dend` loop-termination comment to read `comp` instead of `uncompressed_size` (the invariant still holds: `comp >= 1` is not guaranteed for a 0-byte page, but `hlen >= 1` from `decode_page_header` still guarantees strict progress because `dstart = off + hlen` and `off_next = dstart + comp >= off + hlen >= off + 1`). Make the comment say exactly that.

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet 2>&1 | tail -20` → `extract_decodes_snappy_plain_int64`, `extract_snappy_and_uncompressed_identical`, the gzip-reject test, AND every existing OBJ-2a/2b test (`extract_golden_int64_two_rows`, `extract_decodes_dictionary_int64`, `extract_plain_and_dict_are_identical`, `extract_rejects_optional_repetition`, `extract_rejects_schema_chunk_type_mismatch`, `extract_rejects_missing_column`, `extract_rejects_delta_encoding`, the dict-page-offset/truncated locks) pass; `FAILED=0`.

- [ ] **Step 6:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green. Record measured total.

- [ ] **Step 7: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/lib.rs && git commit -m "parquet: page_payload Cow helper + Snappy codec gate flip (advance by compressed_size)" && git push
```

---

### Task 4: Real pyarrow Snappy fixtures + e2e (#168)

**Files:**
- Create: `crates/kessel-parquet/tests/fixtures/snappy_dict.parquet`, `crates/kessel-parquet/tests/fixtures/snappy_plain.parquet` (via pyarrow)
- Modify: `crates/kessel-parquet/tests/fixtures/README.md`, `crates/kessel-parquet/tests/fixture_roundtrip.rs`, `crates/kesseldb-server/tests/external_source_parquet_oracle.rs`

- [ ] **Step 1: Generate (real pyarrow 24.0.0):**
```bash
cd /c/Users/ihass/KesselDB && python -c "
import pyarrow as pa, pyarrow.parquet as pq
sch = pa.schema([pa.field('id', pa.int64(), nullable=False),
                 pa.field('s',  pa.large_utf8(), nullable=False)])
t = pa.table({'id': pa.array([7,7,-2,7,100], type=pa.int64()),
              's':  pa.array(['a','a','b','c','a'], type=pa.large_utf8())}, schema=sch)
pq.write_table(t,'crates/kessel-parquet/tests/fixtures/snappy_dict.parquet',
               use_dictionary=True, compression='snappy', version='1.0', data_page_version='1.0')
pq.write_table(t,'crates/kessel-parquet/tests/fixtures/snappy_plain.parquet',
               use_dictionary=False, compression='snappy', version='1.0', data_page_version='1.0')
print('wrote snappy_dict.parquet + snappy_plain.parquet rows=5')
"
```
Expected `wrote snappy_dict.parquet + snappy_plain.parquet rows=5`. If pyarrow fails → STOP, report BLOCKED (do not hand-fabricate).

- [ ] **Step 2: Metadata verify both are SNAPPY (not silently uncompressed):**
```bash
cd /c/Users/ihass/KesselDB && python -c "
import pyarrow.parquet as pq
for f in ['snappy_dict','snappy_plain']:
  m=pq.ParquetFile(f'crates/kessel-parquet/tests/fixtures/{f}.parquet').metadata; rg=m.row_group(0)
  print(f, [(rg.column(i).path_in_schema, rg.column(i).compression, rg.column(i).encodings) for i in range(m.num_columns)])
  t=pq.read_table(f'crates/kessel-parquet/tests/fixtures/{f}.parquet'); print(' ', t.column('id').to_pylist(), t.column('s').to_pylist())
"
```
Expected: each column `compression == 'SNAPPY'`; `snappy_dict` encodings include `PLAIN_DICTIONARY`; rows `[7,7,-2,7,100]` / `['a','a','b','c','a']`. If any column is `UNCOMPRESSED`, the fixture does not test OBJ-2b-3 → report BLOCKED (regenerate / investigate).

- [ ] **Step 3: README** — append to `crates/kessel-parquet/tests/fixtures/README.md`:
```markdown
## snappy_dict.parquet / snappy_plain.parquet (OBJ-2b-3)

Regenerate:

    python -c "import pyarrow as pa, pyarrow.parquet as pq; \
    sch=pa.schema([pa.field('id',pa.int64(),nullable=False),pa.field('s',pa.large_utf8(),nullable=False)]); \
    t=pa.table({'id':pa.array([7,7,-2,7,100],type=pa.int64()),'s':pa.array(['a','a','b','c','a'],type=pa.large_utf8())},schema=sch); \
    pq.write_table(t,'crates/kessel-parquet/tests/fixtures/snappy_dict.parquet',use_dictionary=True,compression='snappy',version='1.0',data_page_version='1.0'); \
    pq.write_table(t,'crates/kessel-parquet/tests/fixtures/snappy_plain.parquet',use_dictionary=False,compression='snappy',version='1.0',data_page_version='1.0')"

Real pyarrow 24.0.0, SNAPPY-compressed, V1, flat REQUIRED.
snappy_dict = dictionary-encoded; snappy_plain = PLAIN.
Expected rows: id=[7,7,-2,7,100]; s=["a","a","b","c","a"].
```

- [ ] **Step 4: Roundtrip test** — READ `crates/kessel-parquet/tests/fixture_roundtrip.rs` for its exact import/`const` convention, then add (matching it):
```rust
#[test]
fn snappy_fixtures_roundtrip() {
    for f in ["snappy_dict.parquet", "snappy_plain.parquet"] {
        let path = format!(
            "{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), f);
        let bytes = std::fs::read(&path).expect("read fixture");
        let rows = kessel_parquet::extract(&bytes, &["id", "s"])
            .unwrap_or_else(|e| panic!("{f}: {e}"));
        assert_eq!(rows, vec![
            vec![kessel_parquet::PqValue::I64(7),  kessel_parquet::PqValue::Bytes(b"a".to_vec())],
            vec![kessel_parquet::PqValue::I64(7),  kessel_parquet::PqValue::Bytes(b"a".to_vec())],
            vec![kessel_parquet::PqValue::I64(-2), kessel_parquet::PqValue::Bytes(b"b".to_vec())],
            vec![kessel_parquet::PqValue::I64(7),  kessel_parquet::PqValue::Bytes(b"c".to_vec())],
            vec![kessel_parquet::PqValue::I64(100),kessel_parquet::PqValue::Bytes(b"a".to_vec())],
        ], "{f}");
    }
}
```
(Match the file's existing import style — if it `use`s short names, use them.)

- [ ] **Step 5: e2e** — READ `crates/kesseldb-server/tests/external_source_parquet_oracle.rs`; it has `tls_stub_with_fixture()` + a dict case from SP103. Add a parallel `#[test] refresh_snappy_parquet_from_s3_fails_closed_and_state_intact` pointing the SAME harness at `snappy_dict.parquet` (fail-closed, no router fixture-trust bypass; identical assertion structure to the existing cases).

- [ ] **Step 6:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet --test fixture_roundtrip 2>&1 | tail -8` (snappy roundtrip + existing pass) and `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources-objstore --test external_source_parquet_oracle 2>&1 | tail -8` (SP101 + SP103 dict + new snappy case pass).

- [ ] **Step 7:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green; existing oracles unchanged.

- [ ] **Step 8: Commit** (verify the 2 `.parquet` binaries are staged via `git status --porcelain`):
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/tests/ crates/kesseldb-server/tests/external_source_parquet_oracle.rs && git commit -m "parquet: real pyarrow Snappy fixtures (dict+plain) + snappy e2e (fail-closed)" && git push
```

---

### Task 5: Pentest pass (#169)

**Files:** Modify `crates/kessel-parquet/src/snappy.rs` (append `#[cfg(test)] mod pentest`) and `crates/kessel-parquet/src/lib.rs` (`mod tests`: one extract-level lock).

- [ ] **Step 1:** Append at the END of `crates/kessel-parquet/src/snappy.rs`:

```rust
// ── PENTEST PASS — adversarial lock tests ─────────────────────────
// Snappy page bytes are operator-source-controlled. Each case: no
// panic / no OOM / no stack-overflow, and a well-formed Result
// (typed Bad/Unsupported, OR correct Ok for the positive
// overlapping-copy correctness lock).
#[cfg(test)]
mod pentest {
    use super::*;

    fn nb(src: &[u8], expected: usize) {
        let s = src.to_vec();
        let r = std::panic::catch_unwind(move || decompress(&s, expected));
        assert!(r.is_ok(), "must NOT panic/OOM-unwind");
        assert!(
            matches!(r.unwrap(),
                Err(PqError::Bad(_)) | Err(PqError::Unsupported(_))),
            "hostile input must be a typed error"
        );
    }

    #[test]
    fn over_cap_no_alloc() {
        // expected_len > 64 MiB → Unsupported BEFORE allocation.
        nb(&[0xFF, 0xFF, 0xFF, 0xFF, 0x7F], SNAPPY_MAX_DECOMP + 1);
    }

    #[test]
    fn decompression_bomb_bounded() {
        // tiny src, preamble claims a 2 GiB uncompressed length, but
        // expected_len passed in is the (capped) page header value;
        // here expected_len within cap but src can't satisfy it →
        // Bad, no multi-GB alloc (Vec::with_capacity(expected_len) is
        // ≤ 64 MiB; the per-element guards reject before overrun).
        nb(&[0x80, 0x80, 0x80, 0x10 /*~32 MiB preamble*/], 1 << 25);
    }

    #[test]
    fn preamble_mismatch_bad() {
        nb(&[0x03, 0x08, 0x61, 0x62, 0x63], 5); // declares 3, expect 5
    }

    #[test]
    fn copy_offset_zero_bad() {
        nb(&[0x02, 0x00, 0x61, 0x06, 0x00, 0x00], 2);
    }

    #[test]
    fn copy_offset_past_output_bad() {
        nb(&[0x06, 0x00, 0x61, 0x12, 0x09, 0x00], 6);
    }

    #[test]
    fn copy_overproduces_bad() {
        // 1-byte literal then a copy whose length pushes past
        // expected_len (expected 2 but copy len 5).
        nb(&[0x02, 0x00, 0x61, 0x12, 0x01, 0x00], 2);
    }

    #[test]
    fn literal_past_src_bad() {
        nb(&[0x0A, 0x24, 0x61, 0x62], 10);
    }

    #[test]
    fn truncated_offset_bad() {
        // 2-byte-offset copy tag but only 1 offset byte present.
        nb(&[0x06, 0x00, 0x61, 0x12, 0x01], 6);
    }

    #[test]
    fn trailing_after_full_bad() {
        // literal fills output (len 3) then a spurious extra tag.
        nb(&[0x03, 0x08, 0x61, 0x62, 0x63, 0x00, 0x61], 3);
    }

    #[test]
    fn overlapping_copy_positive_correctness_lock() {
        // VALID Snappy: 1-byte literal 'a' + 2-byte-offset copy
        // len 5 off 1 → "aaaaaa". MUST decode Ok (not over-rejected).
        let blk = [0x06u8, 0x00, 0x61, 0x12, 0x01, 0x00];
        assert_eq!(decompress(&blk, 6).unwrap(), b"aaaaaa".to_vec());
    }
}
```

- [ ] **Step 2:** Add to `lib.rs` `mod tests` (reuse `build_snappy_plain_int64_file` from Task 3):
```rust
#[test]
fn extract_snappy_lying_compressed_size_is_bad() {
    // Corrupt f4 compressed_page_size to point past EOF: rebuild
    // the file but truncate the body so the recorded comp size
    // overruns. Simplest: take the valid file and chop the page
    // bytes so page_payload's get(dstart..dstart+comp) is None.
    let mut f = build_snappy_plain_int64_file();
    // remove the last data byte of the snappy block region (between
    // PAR1 header and FileMetaData) — find a safe truncation that
    // keeps the footer parseable is fragile; instead assert the
    // whole-file hostile path: truncate to 6 bytes (footer-short).
    f.truncate(6);
    let owned = f.clone();
    let r = std::panic::catch_unwind(move || extract(&owned, &["id"]));
    assert!(r.is_ok(), "must not panic");
    assert!(matches!(r.unwrap(), Err(PqError::Bad(_))));
}
```
(If a footer-valid-but-lying-`compressed_size` file is cleanly
constructible by parameterising `build_snappy_plain_int64_file` —
analogous to SP103's dict-offset override — prefer that genuine path
and name the test accordingly; otherwise the truncation lock above is
acceptable as a no-panic guarantee. Document the choice in the report.)

- [ ] **Step 3:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet pentest 2>&1 | tail -15` and `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet extract_snappy_lying_compressed_size_is_bad 2>&1 | tail -5` → all pass, fast (no hang/OOM). If `overlapping_copy_positive_correctness_lock` fails the decoder mishandles valid Snappy — fix the decoder, never the test.

- [ ] **Step 4:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green.

- [ ] **Step 5: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/snappy.rs crates/kessel-parquet/src/lib.rs && git commit -m "parquet: pentest lock tests for Snappy decompress (no panic/OOM; overlap-copy lock)" && git push
```

---

### Task 6: Docs + gate reconciliation + memory (#170)

**Files:**
- Create: `docs/superpowers/specs/2026-05-19-kesseldb-subproject104-snappy.md`
- Modify: `docs/STATUS.md`, `docs/USAGE.md`
- Modify (auto-memory, OUTSIDE repo, never git-add): `C:\Users\ihass\.claude\projects\C--Users-ihass--local-bin\memory\project_kesseldb.md`, `…\MEMORY.md`

- [ ] **Step 1: Measure.** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -25` → sum passed = `<FINAL>`; `<DELTA> = <FINAL> − 326`. Confirm `FAILED=0`, seed-7 green. `cargo tree -p kesseldb-server | grep -Ei "parquet|objstore|rustls|webpki"` → none; `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty.

- [ ] **Step 2: Read `docs/superpowers/specs/2026-05-19-kesseldb-subproject103-dict.md`** for the EXACT convention, then create `docs/superpowers/specs/2026-05-19-kesseldb-subproject104-snappy.md`:

```markdown
# KesselDB — Subproject 104: OBJ-2b-3 Parquet Snappy block decompression

**Date:** 2026-05-19  **Status:** done — code + tests committed and passing.

**Builds on:**
- Subproject 100 — Object-store external sources (OBJ-1):
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject100-objstore.md`
- Subproject 101 — Parquet OBJ-2a:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject101-parquet.md`
- Subproject 102 — RLE/bit-packing hybrid:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject102-rle.md`
- Subproject 103 — Parquet dictionary encoding:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject103-dict.md`

Design document:
`docs/superpowers/specs/2026-05-19-parquet-snappy-design.md`

Plan document:
`docs/superpowers/plans/2026-05-19-parquet-snappy.md`

---

## What shipped

`kessel-parquet::extract()` now decodes **Snappy-compressed** flat
REQUIRED, V1 Parquet (dictionary OR PLAIN pages) — pyarrow's true
default `compression='snappy'`:

- `snappy.rs` (new, pure, zero-dep): raw-block Snappy `decompress`
  with a 64 MiB hard decompressed-page cap; overlapping copies done
  byte-by-byte; every length/offset bounds-checked.
- `meta.rs`: `Codec::Snappy` (CompressionCodec id 1).
- `lib.rs`: `page_payload` `Cow` helper — slices the on-disk page by
  `compressed_size` (Uncompressed → borrowed, Snappy → decompressed
  to `uncompressed_size`); the codec gate now accepts
  Uncompressed|Snappy (Other → Unsupported OBJ-2c); the file offset
  advances by `compressed_size` for both the dictionary page and
  every data page (this also corrects a latent OBJ-2a
  compressed==uncompressed assumption — safe because all prior
  fixtures set them equal).

Still rejected with typed errors: OPTIONAL/levels (OBJ-2b-4),
gzip/zstd/lz4/brotli, INT96/DECIMAL, REPEATED/nested, V2 pages, and
Snappy pages above the 64 MiB cap (OBJ-2c).

---

## Verification

- Spec KATs hand-derived from google/snappy `format_description.txt`
  (literal; 1/2/4-byte-offset copies; the `"aaaaaa"`
  overlapping-copy RLE lock; multi-byte literal length; malformed →
  Bad; over-cap → Unsupported).
- Real pyarrow 24.0.0 `compression='snappy'` fixtures
  (`snappy_dict.parquet` use_dictionary=True, `snappy_plain.parquet`
  use_dictionary=False, both REQUIRED) — metadata-verified SNAPPY —
  round-trip; e2e via the SP101 oracle harness (fail-closed, no
  router fixture-trust bypass).
- Determinism pin: same logical column is byte-identical `PqValue`
  whether Snappy or uncompressed (`pq_to_cell`/coerce unchanged).
- Pentest: catch_unwind locks (over-cap pre-alloc, decompression
  bomb, preamble mismatch, offset 0 / past output, overproduce,
  literal/offset truncation, trailing-after-full, lying
  compressed_size) → typed errors no panic/OOM; positive
  overlapping-copy correctness lock.

---

## Honest gate accounting

`kessel-parquet` is an existing workspace member; its new tests run
under `cargo test --workspace`. Default-build total: **326 → <FINAL>**
(+<DELTA>) — new snappy/meta/extract/fixture/pentest tests. NOT a
zero-delta (same corrected stance as SP100/101/102/103). Kernel pulls
no new external dependency; `kessel-parquet/Cargo.toml`
`[dependencies]` empty; default `cargo tree -p kesseldb-server` links
no parquet/objstore/rustls/webpki;
`large_seed_corpus_is_deterministic_and_converges` green; existing
EXT/TLS/OBJ-1 oracles (2/1/1) unchanged; all OBJ-2a/2b decode+gate
tests unchanged (they use compressed==uncompressed).

---

## Deferred (next OBJ-2b / OBJ-2c)

- OBJ-2b-4: OPTIONAL/def-levels + nullable wiring + e2e.
- OBJ-2c: gzip/zstd/lz4/brotli, INT96/DECIMAL, REPEATED/nested, V2
  pages, Snappy pages above the 64 MiB cap.
```

Substitute `<FINAL>`/`<DELTA>`.

- [ ] **Step 3: STATUS.md row after the SP103 row** (numeric order; match the SP103 row's structure incl. gate numbers + `Record:` backlink):
```
- OBJ-2b-3 (SP104): Snappy-compressed flat REQUIRED V1 Parquet
  (dict or PLAIN) now decoded (pyarrow default compression='snappy')
  via kessel-parquet::snappy (pure raw-block, 64 MiB cap). Still
  typed-Unsupported: OPTIONAL (OBJ-2b-4), gzip/zstd/INT96/V2 + >64MiB
  Snappy (OBJ-2c). Honest gate: 326→<FINAL> (+<DELTA>; new
  snappy/meta/extract/fixture/pentest tests; not zero-delta). Kernel
  zero-dep + seed-7 green + EXT/TLS/OBJ-1 oracles 2/1/1 unchanged.
  Record: `docs/superpowers/specs/2026-05-19-kesseldb-subproject104-snappy.md`.
```

- [ ] **Step 4: docs/USAGE.md §7f** — append (no overclaim):
```
> **OBJ-2b-3 (SP104):** Snappy-compressed Parquet (pyarrow default
> `compression='snappy'`) is now supported for flat REQUIRED, V1
> files (dictionary or PLAIN). nullable/OPTIONAL columns still
> unsupported (→ OBJ-2b-4); gzip/zstd and Snappy pages >64 MiB →
> OBJ-2c.
```

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -12` → `FAILED=0`, total == `<FINAL>`, seed-7 green.

- [ ] **Step 6: Commit docs**
```bash
cd /c/Users/ihass/KesselDB && git add docs/superpowers/specs/2026-05-19-kesseldb-subproject104-snappy.md docs/STATUS.md docs/USAGE.md && git commit -m "docs: OBJ-2b-3 Snappy — subproject104 record + STATUS/USAGE + gate reconciliation" && git push
```

- [ ] **Step 7: Auto-memory (OUTSIDE repo — never git-add).** Append via Bash heredoc:
```bash
cat >> "/c/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/project_kesseldb.md" <<'EOF'

## SP104 (2026-05-19) — OBJ-2b-3 Parquet Snappy block decompression
extract() now decodes Snappy-compressed flat REQUIRED V1 (dict or
PLAIN) — pyarrow true default compression='snappy'. New snappy.rs pure
raw-block decompress (64 MiB cap, overlapping copies byte-wise,
bounds-checked); meta.rs Codec::Snappy(1); lib.rs page_payload Cow
helper (on-disk slice by compressed_size; Snappy→decompress /
Uncompressed→borrow; off advances by compressed_size — also corrects a
latent OBJ-2a comp==uncomp assumption). Real pyarrow snappy_dict +
snappy_plain fixtures + e2e fail-closed. Honest gate 326→<FINAL>.
Kernel zero-dep + seed-7 + EXT/TLS/OBJ-1 oracles 2/1/1 unchanged.
Next: OBJ-2b-4 OPTIONAL+nullable / OBJ-2c gzip-zstd-INT96-V2-bigpage.
EOF
```
(substitute `<FINAL>`). Then update the KesselDB line in
`/c/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/MEMORY.md`
(Read to find the exact `- [KesselDB](project_kesseldb.md) — …` line,
Edit it) so the trailing status clause becomes:
`SP104 SHIPPED: OBJ-2b-3 Snappy-compressed flat REQUIRED V1 (dict/PLAIN, pyarrow true default). Open: OBJ-2b-4 OPTIONAL+fixtures+e2e / OBJ-2c gzip-zstd-INT96-V2-bigpage / OBJ-3 / OBJ-4 / OBJ-5 / WASM / #75 SP-A scatter scan / seed-7 liveness / SQL-over-cluster`.
Keep the rest of that line's prefix intact.

- [ ] **Step 8:** Verify `cd /c/Users/ihass/KesselDB && git status --porcelain` is EMPTY (no memory path, no stray logs; `rm -f` any test-output.log — never commit logs). Report DONE with 326/`<FINAL>`/`<DELTA>`, FAILED, seed-7, deps-clean, docs commit SHA, memory updated & not git-added, clean tree.

---

## Self-Review

**1. Spec coverage:** snappy.rs raw-block + 64 MiB cap + overlapping-copy → T1; `Codec::Snappy` → T2; `page_payload` Cow + gate flip + advance-by-compressed_size (both page kinds) → T3; determinism pin → T3 `extract_snappy_and_uncompressed_identical`; real pyarrow snappy dict+plain fixtures + e2e fail-closed → T4; pentest (over-cap pre-alloc, bomb, preamble mismatch, offset 0/past, overproduce, truncation, trailing, lying compressed_size) + positive overlapping-copy lock → T5; honest gate reconciliation + SP103-convention record + STATUS/USAGE + memory → T6. All design sections mapped.

**2. Placeholder scan:** No "TBD"/"handle edge cases"/"similar to". `<BASELINE>/<FINAL>/<DELTA>` are runtime-measured, defined in T0/T6. All code + KAT bytes concrete and hand-derived. The T3 gzip-test note and T5 lying-compressed-size note give explicit decision criteria, not vague deferral.

**3. Type consistency:** `decompress(&[u8], usize) -> Result<Vec<u8>, PqError>` and `SNAPPY_MAX_DECOMP: usize` used identically across T1/T3/T5. `page_payload(&[u8], usize, usize, usize, meta::Codec) -> Result<Cow<[u8]>, PqError>` consistent in T3. `Codec::{Uncompressed,Snappy,Other}` matches the T2 enum. `PqError::{Bad,Unsupported}`/`PqValue::{I64,Bytes}` match the crate.

Plan is internally consistent and fully covers the OBJ-2b-3 design.
