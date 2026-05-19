# OBJ-2c-1 Parquet GZIP Page Decompression Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decompress Parquet GZIP pages (pyarrow `compression='gzip'`) with a pure zero-dependency RFC 1952 + RFC 1951 inflater, flipping the GZIP codec gate so the existing PLAIN/dict/def-level decode runs over the decompressed bytes unchanged.

**Architecture:** New pure module `crates/kessel-parquet/src/gzip.rs` (RFC 1952 wrapper parse + RFC 1951 inflate + CRC32 verify + a 64 MiB hard cap). `meta.rs` learns `Codec::Gzip`. `lib.rs::page_payload` (the single decompression seam from SP104) gains a `Gzip` arm; the codec gate accepts it. Everything downstream is unchanged — GZIP composes automatically with dictionary, OPTIONAL/def-levels, and multi-page. No kessel-fetch/kessel-sql/server/kernel change.

**Tech Stack:** Rust (workspace edition), `#![forbid(unsafe_code)]`, zero external deps, existing `PqError`/`PqValue`, `std::borrow::Cow`.

---

## Context for the implementer (read once)

`crates/kessel-parquet` modules: `thrift.rs`, `footer.rs`, `meta.rs`, `plain.rs`, `rle.rs`, `dict.rs`, `snappy.rs`, `lib.rs`. `Cargo.toml` `[dependencies]` is empty and MUST stay empty.

- `meta.rs` `Codec` is `enum Codec { Uncompressed, Snappy, Other(i32) }`, `from_i32`: `0=>Uncompressed, 1=>Snappy, o=>Other(o)`.
- `lib.rs:50 fn page_payload<'a>(file,dstart,comp,uncomp,codec) -> Result<Cow<'a,[u8]>,PqError>`: arms `Uncompressed => Cow::Borrowed`, `Snappy => Cow::Owned(snappy::decompress(on_disk, uncomp)?)`, `Other(_) => Unsupported`. The on-disk slice is `file[dstart..dstart+comp]` (checked).
- `lib.rs:154` codec gate in `read_chunk_values`: `match cc.codec { Uncompressed | Snappy => {}, Other(_) => return Err(Unsupported("compression codec ...: OBJ-2c")) }`.
- `lib.rs:601 fn extract_rejects_gzip_codec_obj2c()` — currently builds a codec-2 (GZIP) file and asserts `Unsupported`. This slice intentionally **supports** GZIP, so this test is repurposed (see Task 3).
- `snappy.rs:19 pub(crate) const SNAPPY_MAX_DECOMP: usize = 64 << 20;` and `snappy::decompress(src,expected_len)` is the structural template to mirror.

**Parquet GZIP** = a single RFC 1952 gzip member: 10-byte header `ID1=0x1f ID2=0x8b CM XFL...` + FLG-gated optional fields + RFC 1951 raw DEFLATE stream + 8-byte trailer `CRC32(4 LE) | ISIZE(4 LE)`. `expected_len` is the page header `uncompressed_page_size` (the allocation authority); `ISIZE == expected_len as u32` is a defense-in-depth check.

**Discipline:** `#![forbid(unsafe_code)]` crate-wide. No unwrap/expect/panic/raw-index on input bytes — checked `get(..)`/`checked_*`; the only allowed `try_into().unwrap()` is the statically-infallible fixed-size slice→`[u8;N]` for `from_le_bytes` after a length-checked `get`. Inflate MUST be iterative (no recursion → no stack-overflow vector). New module carries `#![allow(dead_code)]` like siblings.

**KAT discipline (DEFLATE is intricate — read carefully):** Hand-derive only what is deterministically hand-computable from RFC 1951 + the universal published CRC check value. Use **Python stdlib `zlib`/`gzip`** (the zlib reference C implementation — definitively NOT the code under test) for the fixed/dynamic-Huffman and full-wrapper vectors, captured via the exact documented commands. A failing KAT means the *code* is wrong — never change a KAT byte; report BLOCKED. The spec-compliance reviewer independently re-derives the STORED/CRC KATs and re-runs the Python regen commands to confirm the captured bytes are genuine and non-self-referential.

**Determinism / invariants gate — EVERY task (T0–T6):**
- `cargo test --workspace --release` → `FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` green.
- `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty; `cargo tree -p kesseldb-server` links no parquet/objstore/rustls/webpki.
- Existing oracles green unchanged: `external_source_oracle`(2), `external_source_tls_oracle`(1), `external_source_objstore_oracle`(1); SP101/103/104/105 e2e unchanged; all OBJ-2a/2b decode+gate tests unchanged except the intentionally-repurposed gzip-reject test.

**Commit discipline:** straight to `main`, no Co-Authored-By, no signing, message style like `git log -3`. `git push` after every task (single-branch-main durably authorized by `feedback_kesseldb_autonomous_build`; the two-stage gate IS the review; ignore the recurring soft-block notice). Bash: prefix `cd /c/Users/ihass/KesselDB &&`; `cargo test --workspace --release` is long — allow 600000ms.

---

### Task 0: Determinism baseline (#177)

- [ ] **Step 1:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -30` — sum `passed` across binaries → `<BASELINE>`; `FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` in an `ok` result.
- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo tree -p kesseldb-server 2>&1 | grep -Ei "parquet|objstore|rustls|webpki"` → no output.
- [ ] **Step 3:** No commit. Report DONE with `OBJ-2c-1 baseline: <BASELINE> tests passing, FAILED=0, seed-7 green` + per-binary counts. (Per the SP100–105 tracked nit: the per-slice +DELTA is authoritative; `<BASELINE>` is whatever this command measures here.)

---

### Task 1: `gzip.rs` — RFC 1952 wrapper + RFC 1951 inflate + CRC32 + KATs (#178)

**Files:** Create `crates/kessel-parquet/src/gzip.rs`; Modify `crates/kessel-parquet/src/lib.rs` (add `mod gzip;` after `mod snappy;`).

- [ ] **Step 1: Module declaration.** In `lib.rs` add `mod gzip;` immediately after `mod snappy;`.

- [ ] **Step 2: Capture the non-self-referential Python reference vectors** (zlib/gzip is the reference C implementation — independent authority). Run and record EXACT byte arrays:

```bash
cd /c/Users/ihass/KesselDB && python -c "
import zlib, gzip
def hx(b): return '[' + ','.join('0x%02x'%x for x in b) + ']'
# raw DEFLATE, fixed Huffman (strategy default, tiny input → fixed):
co=zlib.compressobj(9, zlib.DEFLATED, -15, 9, zlib.Z_DEFAULT_STRATEGY)
fixed=co.compress(b'hello world')+co.flush()
print('FIXED_DEFLATE', hx(fixed), 'plain=b\"hello world\" len', len('hello world'))
# raw DEFLATE forced dynamic Huffman on a more varied payload:
payload=bytes((i*7+3)%251 for i in range(400))
co=zlib.compressobj(9, zlib.DEFLATED, -15, 9, zlib.Z_DEFAULT_STRATEGY)
dyn=co.compress(payload)+co.flush()
print('DYN_DEFLATE', hx(dyn), 'plainlen', len(payload))
# full gzip member (RFC 1952 wrapper) of b'AB':
print('GZIP_AB', hx(gzip.compress(b'AB')), 'plain=b\"AB\" len 2')
# canonical CRC-32 check value:
print('CRC_CHECK', hex(zlib.crc32(b'123456789') & 0xffffffff))
"
```
Expected `CRC_CHECK 0xcbf43926` (the universal CRC-32/ISO-HDLC check value — if Python prints anything else, STOP/BLOCKED, the environment is broken). Record `FIXED_DEFLATE`, `DYN_DEFLATE`, `GZIP_AB`, and the `payload` recipe (`bytes((i*7+3)%251 for i in range(400))`) verbatim into the KATs below. The reviewer re-runs this exact command to confirm.

- [ ] **Step 3: Write the failing test file.** Create `crates/kessel-parquet/src/gzip.rs` with ONLY this (the `tests` module references not-yet-existing items → red). Paste the Step-2 byte arrays where indicated:

```rust
//! Pure RFC 1952 (gzip member) + RFC 1951 (DEFLATE inflate)
//! decompressor for Parquet GZIP pages. Zero deps, iterative
//! (no recursion), bounds-checked, 64 MiB hard cap, CRC32-verified.
//! Never panics / OOM-aborts / stack-overflows.
#![allow(dead_code)]

use crate::PqError;

fn bad(s: &str) -> PqError { PqError::Bad(s.to_string()) }

/// Hard cap on a single decompressed page. Mirrors
/// snappy::SNAPPY_MAX_DECOMP (same value & rationale; separate const
/// so gzip.rs stays self-contained — the sibling-module convention).
pub(crate) const GZIP_MAX_DECOMP: usize = 64 << 20; // 64 MiB

#[cfg(test)]
mod tests {
    use super::*;

    // CRC-32/ISO-HDLC universal check value (RFC 3309 / zlib): the
    // CRC of b"123456789" is 0xCBF43926. Independent published
    // authority — pins crc32() non-self-referentially.
    #[test]
    fn kat_crc32_canonical_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    // RFC 1951 §3.2.4 STORED block, hand-derived:
    // first DEFLATE byte low bits = BFINAL(1) | BTYPE(00) = 0b001
    //   → byte 0x01 (remaining bits ignored / skip to byte boundary)
    // LEN = 5 (LE u16) = 05 00 ; NLEN = !5 = 0xFFFA (LE) = FA FF
    // then 5 raw bytes "hello".
    #[test]
    fn kat_inflate_stored_block() {
        let deflate = [
            0x01, 0x05, 0x00, 0xFA, 0xFF,
            b'h', b'e', b'l', b'l', b'o',
        ];
        assert_eq!(inflate(&deflate, 5).unwrap(), b"hello".to_vec());
    }

    // Python zlib reference (raw DEFLATE, fixed Huffman) of
    // b"hello world" — captured via the Task-1 Step-2 command.
    #[test]
    fn kat_inflate_fixed_huffman() {
        let deflate: &[u8] = &/*FIXED_DEFLATE bytes here*/;
        assert_eq!(
            inflate(deflate, 11).unwrap(),
            b"hello world".to_vec()
        );
    }

    // Python zlib reference (raw DEFLATE, dynamic Huffman) of
    // payload = bytes((i*7+3)%251 for i in range(400)).
    #[test]
    fn kat_inflate_dynamic_huffman() {
        let deflate: &[u8] = &/*DYN_DEFLATE bytes here*/;
        let want: Vec<u8> =
            (0..400u32).map(|i| ((i * 7 + 3) % 251) as u8).collect();
        assert_eq!(inflate(deflate, 400).unwrap(), want);
    }

    // Overlapping back-reference (RLE) correctness: a STORED seed
    // byte then a fixed-Huffman copy with distance<length. Capture
    // the exact bytes from zlib for b"aaaaaaaa" (8 'a's compress to
    // a literal + a length/distance copy with distance 1):
    //   python -c "import zlib;co=zlib.compressobj(9,8,-15);
    //   import sys;sys.stdout.buffer.write(co.compress(b'a'*8)+co.flush())"
    // Paste the bytes; assert decompresses to 8 'a's (proves
    // byte-wise overlapping copy).
    #[test]
    fn kat_inflate_overlapping_backref() {
        let deflate: &[u8] = &/*RLE_DEFLATE bytes (zlib of b"a"*8)*/;
        assert_eq!(inflate(deflate, 8).unwrap(), vec![b'a'; 8]);
    }

    // Full RFC 1952 gzip member of b"AB" (python gzip.compress).
    #[test]
    fn kat_decompress_gzip_member() {
        let member: &[u8] = &/*GZIP_AB bytes here*/;
        assert_eq!(decompress(member, 2).unwrap(), b"AB".to_vec());
    }

    // ISIZE mismatch → Bad. Take GZIP_AB, pass wrong expected_len.
    #[test]
    fn kat_isize_mismatch_is_bad() {
        let member: &[u8] = &/*GZIP_AB bytes here*/;
        assert!(matches!(decompress(member, 99), Err(PqError::Bad(_))));
    }

    // Over-cap → Unsupported BEFORE allocation.
    #[test]
    fn kat_over_cap_is_unsupported() {
        let member: &[u8] = &/*GZIP_AB bytes here*/;
        assert!(matches!(
            decompress(member, GZIP_MAX_DECOMP + 1),
            Err(PqError::Unsupported(_))
        ));
    }
}
```

- [ ] **Step 4: Run to verify it fails.** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet gzip:: 2>&1 | tail -10` → compile error (`crc32`/`inflate`/`decompress` missing).

- [ ] **Step 5: Implement** — insert above `#[cfg(test)] mod tests`. Implement, in order:
  - `fn crc32(data: &[u8]) -> u32` — CRC-32/ISO-HDLC, poly `0xEDB88320`, build a 256-entry table at call start (a `let mut tbl=[0u32;256]; for n in 0..256 { let mut c=n as u32; for _ in 0..8 { c = if c&1==1 {0xEDB88320 ^ (c>>1)} else {c>>1}; } tbl[n]=c; }`), then `let mut c=0xFFFF_FFFFu32; for &b in data { c = tbl[((c ^ b as u32) & 0xff) as usize] ^ (c>>8); } c ^ 0xFFFF_FFFF`. (`tbl[...]` indexes a local `[u32;256]` we built — not input bytes; safe.)
  - `fn inflate(deflate: &[u8], expected_len: usize) -> Result<Vec<u8>, PqError>` — an iterative RFC 1951 inflater:
    - A `BitReader { data:&[u8], byte:usize, bit:u32 }` with `bits(n)->Result<u32,PqError>` (LSB-first, bounds-checked `data.get(byte)`), `align_to_byte()`.
    - `let mut out = Vec::with_capacity(expected_len);`
    - loop: `bfinal = br.bits(1)?`; `btype = br.bits(2)?`; match `btype`:
      - `0`: `br.align_to_byte()`; read `len`=`u16` LE (two `br.bits(8)`? no — after align, read 2 bytes via the underlying slice through `bits(16)` LSB then `bits(16)`), `nlen`; require `nlen == !len & 0xFFFF` else `Bad`; for each of `len` bytes `push` `br.bits(8)? as u8` (overproduce guard: `out.len() < expected_len` else `Bad`).
      - `1`: build the RFC §3.2.6 fixed lit/len code lengths (`0..=143`→8, `144..=255`→9, `256..=279`→7, `280..=287`→8) and the fixed 5-bit distance lengths (`0..=29`→5), then run the symbol loop (below).
      - `2`: `hlit=br.bits(5)?+257`, `hdist=br.bits(5)?+1`, `hclen=br.bits(4)?+4`; read `hclen` 3-bit lengths into the RFC permutation order `[16,17,18,0,8,7,9,6,10,5,11,4,12,3,13,2,14,1,15]`; build the code-length canonical Huffman; decode `hlit+hdist` code lengths (symbols `0..=15` literal length; `16`=copy-prev 3+`bits(2)`; `17`=zero 3+`bits(3)`; `18`=zero 11+`bits(7)`; bound the running count to `hlit+hdist` else `Bad`); split into lit/len lengths (`hlit`) and dist lengths (`hdist`); build both canonical Huffman decoders.
      - `3`: `Err(Bad("deflate reserved block type"))`.
    - **canonical Huffman**: a `fn build(lens:&[u8]) -> Result<Huff,PqError>` computing `bl_count` then `next_code` (RFC §3.2.2) and storing, per symbol with `len>0`, its canonical code; `fn decode(&self, br) -> Result<u16,PqError>` reads bits one at a time accumulating `code` and `len`, matching against the per-length first-code ranges; reject an over-subscribed/incomplete table and a code that exhausts `MAX_BITS=15` without a match (`Bad`).
    - **symbol loop** (for fixed/dynamic): `let s = litlen.decode(br)?;` if `s<256` push `s as u8` (overproduce guard); if `s==256` break this block; if `257..=285` → `length = LEN_BASE[s-257] + br.bits(LEN_EXTRA[s-257])?` then `let d = dist.decode(br)?; let distance = DIST_BASE[d] + br.bits(DIST_EXTRA[d])?;` require `distance>=1 && (distance as usize)<=out.len()` else `Bad`; `for _ in 0..length { if out.len()>=expected_len {return Err(bad("deflate overproduce"))} let b = out[out.len()-distance as usize]; out.push(b); }` (byte-wise → overlapping `distance<length` correct); `s>285` → `Bad`. The `LEN_BASE/LEN_EXTRA/DIST_BASE/DIST_EXTRA` are the fixed RFC 1951 §3.2.5 tables (29 length codes 257..285, 30 distance codes 0..29) — include them as `const` arrays exactly per the RFC.
    - after a block: if `bfinal==1` break the outer loop. At end: `if out.len()!=expected_len { Err(bad("deflate length mismatch")) } else Ok(out)`.
  - `pub fn decompress(src:&[u8], expected_len:usize) -> Result<Vec<u8>,PqError>`:
    - `if expected_len > GZIP_MAX_DECOMP { return Err(PqError::Unsupported(format!("gzip page {expected_len} exceeds {GZIP_MAX_DECOMP} cap: OBJ-2c"))) }` (before any alloc).
    - require `src.len() >= 18` (10 header + ≥0 deflate + 8 trailer; use checked gets), `src[0]==0x1f && src[1]==0x8b` else `Bad("gzip magic")`; `src[2]==8` else `Unsupported("gzip method != deflate: OBJ-2c")`; `flg=src[3]`; `pos=10`; if `flg&0x04`(FEXTRA): `xlen=u16 LE at pos`, `pos += 2 + xlen` (checked); if `flg&0x08`(FNAME): advance past a NUL (checked, bounded by `src.len()-8`); if `flg&0x10`(FCOMMENT): same; if `flg&0x02`(FHCRC): `pos+=2`. Require `pos <= src.len()-8`. `let trailer=&src[src.len()-8..]; let crc=u32 LE; let isize=u32 LE`. `if isize != (expected_len as u32) { Bad("gzip isize") }`. `let deflate=&src[pos..src.len()-8]; let out=inflate(deflate, expected_len)?; if crc32(&out)!=crc { return Err(bad("gzip crc mismatch")) } Ok(out)`.

- [ ] **Step 6:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet gzip::tests 2>&1 | tail -15` → all KATs pass. If `kat_inflate_overlapping_backref` fails you used a slice/`copy_within` instead of byte-wise push — fix the code, never the KAT.

- [ ] **Step 7:** Full gate: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, total ≥ baseline + (#KATs), seed-7 green.

- [ ] **Step 8: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/gzip.rs crates/kessel-parquet/src/lib.rs && git commit -m "parquet: pure RFC1952+RFC1951 gzip/inflate decompressor (gzip::decompress) + KATs" && git push
```

---

### Task 2: `meta.rs` `Codec::Gzip` (#179)

**Files:** Modify `crates/kessel-parquet/src/meta.rs`.

- [ ] **Step 1: Failing test** — add inside `meta.rs` `#[cfg(test)] mod tests` (has `uv`/`zz`); reuse the `build(codec)` helper pattern from `columnmeta_decodes_snappy_codec` (or replicate it minimally) to assert codec 2→`Codec::Gzip`, codec 6→`Codec::Other(6)`:

```rust
#[test]
fn columnmeta_decodes_gzip_codec() {
    fn build(codec: i64) -> Vec<u8> {
        let mut b = Vec::new();
        b.push(0x15); uv(&mut b, zz(1));                 // f1 version=1
        b.push(0x19); b.push(0x2c);                      // f2 list<struct> 2
        b.push(0x48); uv(&mut b, 4); b.extend_from_slice(b"root");
        b.push(0x15); uv(&mut b, zz(1));                 // num_children=1
        b.push(0x00);
        b.push(0x15); uv(&mut b, zz(2));                 // leaf type=INT64
        b.push(0x25); uv(&mut b, zz(0));                 // repetition=REQUIRED
        b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
        b.push(0x00);
        b.push(0x16); uv(&mut b, zz(1));                 // num_rows=1
        b.push(0x19); b.push(0x1c);                      // list<RowGroup> 1
        b.push(0x19); b.push(0x1c);                      // RG list<ColumnChunk> 1
        b.push(0x3c);                                    // ColumnChunk f3 CMD
        b.push(0x15); uv(&mut b, zz(2));                 // CMD type=INT64
        b.push(0x19); b.push(0x15); uv(&mut b, zz(0));   // encodings [PLAIN]
        b.push(0x19); b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
        b.push(0x15); uv(&mut b, zz(codec));             // f4 codec
        b.push(0x16); uv(&mut b, zz(1));                 // num_values=1
        b.push(0x46); uv(&mut b, zz(4));                 // data_page_offset=4
        b.push(0x00); b.push(0x00);
        b.push(0x26); uv(&mut b, zz(1));                 // RG num_rows=1
        b.push(0x00); b.push(0x00);
        b
    }
    assert_eq!(
        FileMetaData::decode(&build(2)).unwrap()
            .row_groups[0].columns[0].codec, Codec::Gzip);
    assert_eq!(
        FileMetaData::decode(&build(6)).unwrap()
            .row_groups[0].columns[0].codec, Codec::Other(6));
}
```

- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet columnmeta_decodes_gzip_codec 2>&1 | tail -8` → compile error (`no variant Gzip`).

- [ ] **Step 3: Implement** — change the `Codec` enum + `from_i32`:
```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    Uncompressed,
    /// SNAPPY (parquet CompressionCodec id = 1), raw block format.
    Snappy,
    /// GZIP (parquet CompressionCodec id = 2), RFC 1952 member.
    Gzip,
    Other(i32),
}
impl Codec {
    fn from_i32(v: i32) -> Codec {
        match v {
            0 => Codec::Uncompressed,
            1 => Codec::Snappy,
            2 => Codec::Gzip,
            o => Codec::Other(o),
        }
    }
}
```

- [ ] **Step 4:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet meta:: 2>&1 | tail -10` → new test + ALL pre-existing meta tests pass (existing tests use codec 0/1/7 → unchanged; the SP104 `columnmeta_decodes_snappy_codec` uses codec 1→Snappy & 7→Other(7), still true).

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, total ≥ prev+1, seed-7 green.

- [ ] **Step 6: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/meta.rs && git commit -m "parquet: meta Codec::Gzip (CompressionCodec id 2)" && git push
```

---

### Task 3: `page_payload` Gzip arm + codec-gate flip + repurpose reject test (#180)

**Files:** Modify `crates/kessel-parquet/src/lib.rs`.

Before coding: READ `page_payload` (`lib.rs:50`), the codec gate (`lib.rs:154`), and `extract_rejects_gzip_codec_obj2c` (`lib.rs:601`) + the SP104 `build_snappy_plain_int64_file*` builders.

- [ ] **Step 1: Failing tests + builder.** Add inside `lib.rs` `mod tests`:

```rust
/// Build a PLAIN INT64 [7,-2] file gzip-compressed (codec=GZIP=2).
/// `gz` is a real RFC-1952 gzip member of the 16 raw PLAIN bytes,
/// captured from python at test-build time and pasted (the same
/// independent-authority discipline as the inflate KATs). Layout
/// mirrors build_snappy_plain_int64_file but codec=2 and the page
/// body = `gz`, compressed_page_size = gz.len(),
/// uncompressed_page_size = 16.
fn build_gzip_plain_int64_file(gz: &[u8]) -> Vec<u8> {
    // ... identical structure to build_snappy_plain_int64_file
    // (read it and mirror), only: f4 codec = zz(2); page body = gz;
    // f2 uncompressed_page_size = 16; f3 compressed_page_size =
    // gz.len(); dp_num_values=2; dp_encoding=PLAIN(0). Spell every
    // byte exactly as the SP104 builder does, swapping codec & body.
    unimplemented!("mirror build_snappy_plain_int64_file; see plan")
}

#[test]
fn extract_decodes_gzip_plain_int64() {
    // gz = python gzip of (7i64 LE ++ (-2)i64 LE) — capture via:
    //   python -c "import gzip,struct,sys;
    //   sys.stdout.buffer.write(gzip.compress(struct.pack('<qq',7,-2)))"
    // paste the exact bytes:
    let gz: &[u8] = &/*GZIP of 7i64,-2i64 (16 raw bytes)*/;
    let f = build_gzip_plain_int64_file(gz);
    assert_eq!(extract(&f, &["id"]).expect("gzip"),
        vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]);
}

#[test]
fn extract_gzip_uncompressed_snappy_identical() {
    // Source-format independence: same logical [7,-2] three ways.
    let gz: &[u8] = &/*same GZIP of 7i64,-2i64 as above*/;
    let g = extract(&build_gzip_plain_int64_file(gz), &["id"]).unwrap();
    let p = extract(&build_parquet_file(0,0,0,false), &["id"]).unwrap();
    let s = extract(&build_snappy_plain_int64_file(), &["id"]).unwrap();
    assert_eq!(g, p);
    assert_eq!(g, s);
    assert_eq!(g, vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]);
}

#[test]
fn extract_rejects_zstd_codec_obj2c() {
    // Repurposed from extract_rejects_gzip_codec_obj2c: GZIP(2) is
    // now SUPPORTED; ZSTD(6) is still Unsupported (OBJ-2c follow-on).
    let f = build_parquet_file(0, 6, 0, false); // codec=ZSTD(6)
    assert!(matches!(extract(&f, &["id"]), Err(PqError::Unsupported(_))),
        "ZSTD codec must be Unsupported (OBJ-2c)");
}
```

The `build_gzip_plain_int64_file` body MUST be fully spelled out (mirror `build_snappy_plain_int64_file` byte-for-byte, changing only `f4 codec` to `zz(2)`, the page body to `gz`, and `compressed_page_size` to `gz.len() as i64`). Capture `gz` from the documented python command and paste the exact byte array. DELETE the old `extract_rejects_gzip_codec_obj2c` (intentionally superseded). Leave every OTHER OBJ-2a/2b test untouched.

- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet extract_decodes_gzip_plain_int64 2>&1 | tail -8` → FAIL (codec gate rejects GZIP).

- [ ] **Step 3: Implement.** In `page_payload` (`lib.rs:50`), add the arm (next to `Snappy`):
```rust
meta::Codec::Gzip => Ok(std::borrow::Cow::Owned(
    gzip::decompress(on_disk, uncomp)?)),
```
In the codec gate (`lib.rs:154`) change `Uncompressed | Snappy => {}` to `Uncompressed | Snappy | Gzip => {}` (the `Other(_)` arm message stays `"compression codec (zstd/lz4/brotli): OBJ-2c"`). NOTHING else changes — the dictionary page + every data page already flow through `page_payload`, so GZIP composes with dict/OPTIONAL/multi-page automatically.

- [ ] **Step 4:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet 2>&1 | tail -25` → `extract_decodes_gzip_plain_int64`, `extract_gzip_uncompressed_snappy_identical`, `extract_rejects_zstd_codec_obj2c` pass; the deleted gzip-reject test gone; EVERY existing OBJ-2a/2b test (golden/dict/snappy/optional/nullable/delta/schema-mismatch/missing-column/truncated/nested) passes; `FAILED=0`.

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green. Record measured total.

- [ ] **Step 6: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/lib.rs && git commit -m "parquet: page_payload Gzip arm + codec gate flip (GZIP supported; intended gzip-reject→zstd-reject test change)" && git push
```

---

### Task 4: Real pyarrow gzip fixtures + e2e (#181)

**Files:** Create `crates/kessel-parquet/tests/fixtures/{gzip_dict,gzip_plain,gzip_nullable}.parquet`; modify `.../README.md`, `crates/kessel-parquet/tests/fixture_roundtrip.rs`, `crates/kesseldb-server/tests/external_source_parquet_oracle.rs`.

- [ ] **Step 1: Generate (real pyarrow 24.0.0):**
```bash
cd /c/Users/ihass/KesselDB && python -c "
import pyarrow as pa, pyarrow.parquet as pq
sch = pa.schema([pa.field('id', pa.int64(), nullable=False),
                 pa.field('s',  pa.large_utf8(), nullable=False)])
t = pa.table({'id': pa.array([7,7,-2,7,100], type=pa.int64()),
              's':  pa.array(['a','a','b','c','a'], type=pa.large_utf8())}, schema=sch)
pq.write_table(t,'crates/kessel-parquet/tests/fixtures/gzip_dict.parquet',
               use_dictionary=True, compression='gzip', version='1.0', data_page_version='1.0')
pq.write_table(t,'crates/kessel-parquet/tests/fixtures/gzip_plain.parquet',
               use_dictionary=False, compression='gzip', version='1.0', data_page_version='1.0')
tn = pa.table({'id': pa.array([7,7,None,-2,100], type=pa.int64()),
               's':  pa.array(['a',None,'b','c','a'], type=pa.large_utf8())})
pq.write_table(tn,'crates/kessel-parquet/tests/fixtures/gzip_nullable.parquet',
               compression='gzip', version='1.0', data_page_version='1.0')
print('wrote gzip_dict + gzip_plain + gzip_nullable rows=5')
"
```
If pyarrow fails → STOP, BLOCKED.

- [ ] **Step 2: Metadata-verify all three are GZIP** (and gzip_nullable is OPTIONAL):
```bash
cd /c/Users/ihass/KesselDB && python -c "
import pyarrow.parquet as pq
for f in ['gzip_dict','gzip_plain','gzip_nullable']:
  pf=pq.ParquetFile(f'crates/kessel-parquet/tests/fixtures/{f}.parquet'); rg=pf.metadata.row_group(0)
  print(f, pf.schema_arrow, [(rg.column(i).compression, rg.column(i).encodings) for i in range(pf.metadata.num_columns)])
  t=pq.read_table(f'crates/kessel-parquet/tests/fixtures/{f}.parquet'); print(' ', t.column('id').to_pylist(), t.column('s').to_pylist())
"
```
Expected: every column compression == `GZIP`; `gzip_nullable` schema fields nullable (OPTIONAL); rows as written (the nullable one has `None` at `id[2]`/`s[1]`). If any column is not GZIP → STOP, BLOCKED (report metadata).

- [ ] **Step 3: README** — append a `## gzip_dict.parquet / gzip_plain.parquet / gzip_nullable.parquet (OBJ-2c-1)` block with the exact regen command + expected rows (mirror the SP104 snappy README entry style).

- [ ] **Step 4: Roundtrip** — READ `fixture_roundtrip.rs` for the convention; add a test loading all three via `kessel_parquet::extract(&bytes,&["id","s"])` asserting: `gzip_dict`/`gzip_plain` → `[[I64(7),Bytes("a")],[I64(7),Bytes("a")],[I64(-2),Bytes("b")],[I64(7),Bytes("c")],[I64(100),Bytes("a")]]`; `gzip_nullable` → `[[I64(7),Bytes("a")],[I64(7),Null],[Null,Bytes("b")],[I64(-2),Bytes("c")],[I64(100),Bytes("a")]]`. (This roundtrip through production `extract()` on metadata-verified-GZIP real pyarrow files is the decisive non-self-referential proof; `gzip_nullable` proves gzip ∘ def-levels ∘ dict composition through `page_payload`.)

- [ ] **Step 5: e2e** — READ `external_source_parquet_oracle.rs`; add `refresh_gzip_parquet_from_s3_fails_closed_and_state_intact` mirroring the SP105 nullable case via the SAME `tls_stub_with_fixture` harness pointed at `gzip_dict.parquet` (fail-closed, NO router fixture-trust bypass; differentiate env/shard/source — e.g. `GPQ`/`gpq`/`gfeed`).

- [ ] **Step 6:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet --test fixture_roundtrip 2>&1 | tail -8` (gzip roundtrips pass) and `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources-objstore --test external_source_parquet_oracle 2>&1 | tail -8` (SP101/103/104/105 + new gzip case pass).

- [ ] **Step 7:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green; existing oracles unchanged.

- [ ] **Step 8: Commit** (verify the 3 `.parquet` binaries staged via `git status --porcelain`):
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/tests/ crates/kesseldb-server/tests/external_source_parquet_oracle.rs && git commit -m "parquet: real pyarrow gzip fixtures (dict+plain+nullable) + gzip e2e (fail-closed)" && git push
```

---

### Task 5: Pentest pass (#182)

**Files:** Modify `crates/kessel-parquet/src/gzip.rs` (append `#[cfg(test)] mod pentest`, AFTER `mod tests` to match the SP104 convention).

- [ ] **Step 1:** Append the pentest module. A `nb(src,expected)` helper (`std::panic::catch_unwind` → assert `r.is_ok()` then `Err(Bad|Unsupported)`), then locks (each hand-built or a documented python-captured corrupt vector):
  - `over_cap`: `decompress(&valid_member, GZIP_MAX_DECOMP+1)` → `Unsupported` (no alloc).
  - `bomb_bounded`: a tiny valid gzip member but `expected_len` within cap that the stream cannot satisfy / ISIZE-inconsistent → `Bad`, no multi-GB alloc.
  - `bad_magic`: `[0x00,0x00,...18 bytes]` → `Bad`.
  - `cm_not_deflate`: a member with `src[2]=0x09` → `Unsupported`.
  - `truncated_header`: `[0x1f,0x8b]` (len 2) → `Bad`.
  - `lying_fextra`: FLG with FEXTRA bit set and `XLEN` huge → `Bad`.
  - `unterminated_fname`: FLG FNAME set, no NUL before trailer → `Bad`.
  - `truncated_deflate`: valid header but DEFLATE cut mid-block → `Bad`.
  - `reserved_btype`: a DEFLATE first byte with `BTYPE==11` → `Bad`.
  - `bad_dynamic_huffman`: a dynamic block whose code-length sequence overruns `HLIT+HDIST` (hand-craft or python-mangle) → `Bad`.
  - `distance_before_output`: a fixed-Huffman stream whose first symbol is a copy with `distance>out.len()` (hand-craft a minimal one, or take the RLE vector and corrupt the distance) → `Bad`.
  - `stored_nlen_mismatch`: STORED block with `NLEN != !LEN` → `Bad`.
  - `isize_mismatch` / `crc_mismatch`: take the valid `GZIP_AB`, flip a trailer byte / pass wrong `expected_len` → `Bad`.
  - **positive correctness locks (assert `Ok` exact):** STORED, fixed-Huffman, dynamic-Huffman, overlapping-backref (reuse the Task-1 KAT vectors); plus a tiny end-to-end: build a gzip member of the 16-byte PLAIN payload and assert `decompress` → those 16 bytes.
  Each wrapped in `catch_unwind`; assert no panic + the expected typed result.

- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet gzip::pentest 2>&1 | tail -15` → all pass FAST (no hang/OOM). If a positive lock fails → BLOCKED (decoder bug, never weaken). If a hostile case panics/OOMs/hangs → BLOCKED (real vuln, exact detail).

- [ ] **Step 3:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green.

- [ ] **Step 4: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/gzip.rs && git commit -m "parquet: pentest lock tests for gzip/inflate (no panic/OOM; overlap-backref + structural locks)" && git push
```

---

### Task 6: Docs + gate reconciliation + memory (#183)

**Files:** Create `docs/superpowers/specs/2026-05-19-kesseldb-subproject106-gzip.md`; modify `docs/STATUS.md`, `docs/USAGE.md`; modify (auto-memory, OUTSIDE repo, never git-add) `…\memory\project_kesseldb.md`, `…\memory\MEMORY.md`.

- [ ] **Step 1: Measure.** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -25` → `<FINAL>`; `<DELTA> = <FINAL> − <BASELINE>` (Task 0's). `FAILED=0`, seed-7 green. `cargo tree -p kesseldb-server | grep -Ei "parquet|objstore|rustls|webpki"` none; `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty.

- [ ] **Step 2:** Read `docs/superpowers/specs/2026-05-19-kesseldb-subproject105-optional.md` for the EXACT convention, then create `docs/superpowers/specs/2026-05-19-kesseldb-subproject106-gzip.md` mirroring it: `# KesselDB — Subproject 106: OBJ-2c-1 Parquet GZIP page decompression`; `**Date:** 2026-05-19  **Status:** done — code + tests committed and passing.`; bare-backtick `Builds on:`/`Design document:`/`Plan document:` lines; `---` separators. Sections: **What shipped** (gzip.rs RFC1952+RFC1951 inflate + CRC32 + cap; Codec::Gzip; page_payload Gzip arm = single-seam → composes with dict/OPTIONAL/multi-page; supported matrix now flat REQUIRED|OPTIONAL + UNCOMPRESSED|Snappy|GZIP + PLAIN|dict + V1); **Intended behavior change (disclosed)** (extract_rejects_gzip_codec_obj2c → extract_rejects_zstd_codec_obj2c; GZIP now supported; all other OBJ-2a/2b tests unchanged); **Verification** (RFC-1951/1952 hand-derived STORED KAT + canonical CRC 0xCBF43926 + python-zlib/gzip reference vectors for fixed/dynamic/overlap/wrapper — non-self-referential; real pyarrow gzip_dict/gzip_plain/gzip_nullable round-trip via production extract(); source-format-independence pin gzip==uncompressed==snappy; e2e fail-closed; pentest); **Honest gate accounting** (`<BASELINE>`→`<FINAL>` +`<DELTA>`; NOT a zero-delta — SP100–105 stance; the per-slice delta is authoritative per the tracked nit; kernel zero-dep; deps empty; seed-7; EXT/TLS/OBJ-1 oracles 2/1/1 unchanged); **Deferred** (OBJ-2c-2 zstd / OBJ-2c-3 V2 pages / OBJ-2c-4 INT96/DECIMAL / OBJ-2c-5 REPEATED-nested / lz4-brotli / >64MiB).

- [ ] **Step 3: STATUS.md** — read it, insert the SP106 row immediately AFTER the SP105 row (numeric order), matching the SP105 row format: gate `<BASELINE>→<FINAL> (+<DELTA>; …; not zero-delta)`, `Record:` backlink, and a clause: `GZIP-compressed Parquet (pyarrow compression='gzip') now reads (RFC1952+RFC1951 zero-dep inflate, CRC32-verified, ≤64MiB) — composes with dict/OPTIONAL via the page_payload seam. Intended change: gzip-reject test → zstd-reject (GZIP now supported). Still typed-Unsupported: zstd/lz4/brotli, INT96/DECIMAL, V2 pages, REPEATED/nested (OBJ-2c-2+).`

- [ ] **Step 4: docs/USAGE.md** — append a §7f `> **OBJ-2c-1 (SP106):**` note (no overclaim) AND update the cumulative "### Parquet scope: what is currently supported (OBJ-2a → OBJ-2b-4)" table: retitle heading to `(OBJ-2a → OBJ-2c-1)`; Compression row → `UNCOMPRESSED, SNAPPY, or GZIP (RFC 1952; pages ≤ 64 MiB decompressed)`; ensure the NOT-supported list still accurately keeps zstd/lz4/brotli, INT96/DECIMAL, V2 pages, REPEATED/nested, >64MiB. No §7f-vs-table contradiction; no stale tag.

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -12` → `FAILED=0`, total == `<FINAL>`, seed-7 green.

- [ ] **Step 6: Commit docs**
```bash
cd /c/Users/ihass/KesselDB && git add docs/superpowers/specs/2026-05-19-kesseldb-subproject106-gzip.md docs/STATUS.md docs/USAGE.md && git commit -m "docs: OBJ-2c-1 GZIP — subproject106 record + STATUS/USAGE cumulative-table + gate reconciliation" && git push
```

- [ ] **Step 7: Auto-memory (OUTSIDE repo — never git-add).** Append via Bash heredoc:
```bash
cat >> "/c/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/project_kesseldb.md" <<'EOF'

## SP106 (2026-05-19) — OBJ-2c-1 Parquet GZIP page decompression
New crates/kessel-parquet/src/gzip.rs: pure RFC1952 wrapper + RFC1951
inflate (stored/fixed/dynamic Huffman bit-at-a-time canonical, byte-wise
overlapping back-ref, iterative no-recursion) + CRC32 verify + 64MiB
GZIP_MAX_DECOMP cap. meta.rs Codec::Gzip(2). lib.rs page_payload Gzip
arm — the single decompression seam, so GZIP composes with dict/OPTIONAL/
multi-page automatically (no other change). Intended change:
extract_rejects_gzip_codec_obj2c → extract_rejects_zstd_codec_obj2c
(GZIP now supported; zstd-6 still Unsupported). Real pyarrow gzip_dict/
gzip_plain/gzip_nullable fixtures + e2e fail-closed + gzip==uncompressed
==snappy source-indep pin. KATs: hand-derived STORED + canonical CRC
0xCBF43926 + python-zlib/gzip reference vectors (non-self-referential).
Honest gate <BASELINE>→<FINAL>. Kernel zero-dep + seed-7 + EXT/TLS/OBJ-1
oracles 2/1/1 unchanged. OBJ-2c arc: 1/5 (gzip done). Next: OBJ-2c-2
zstd / OBJ-2c-3 V2 pages / OBJ-2c-4 INT96-DECIMAL / OBJ-2c-5 REPEATED-
nested / OBJ-3 Iceberg / OBJ-4 listing / OBJ-5 STS-SAS / WASM / #75 SP-A.
EOF
```
(substitute `<BASELINE>`/`<FINAL>`). Then update the KesselDB line in `/c/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/MEMORY.md` (Read to find the exact `- [KesselDB](project_kesseldb.md) — …` line, Edit it) so the trailing status clause becomes: `SP106 SHIPPED: OBJ-2c-1 GZIP page decompression (pyarrow compression='gzip' reads; zero-dep RFC1952+RFC1951, CRC32, ≤64MiB; page_payload seam composes w/ dict/OPTIONAL). Open: OBJ-2c-2 zstd / OBJ-2c-3 V2 pages / OBJ-2c-4 INT96-DECIMAL / OBJ-2c-5 REPEATED-nested / OBJ-3 Iceberg / OBJ-4 listing / OBJ-5 STS-SAS / WASM / #75 SP-A scatter scan / seed-7 liveness / SQL-over-cluster`. Keep the line's existing prefix intact.

- [ ] **Step 8:** `cd /c/Users/ihass/KesselDB && git status --porcelain` EMPTY (no memory path, no stray logs; `rm -f` any test-output.log). Report DONE with `<BASELINE>`/`<FINAL>`/`<DELTA>`, FAILED, seed-7, deps-clean, disclosures present, docs commit SHA, memory updated & not git-added, clean tree.

---

## Self-Review

**1. Spec coverage:** gzip.rs RFC1952+RFC1951 inflate + CRC32 + cap → T1; Codec::Gzip → T2; page_payload Gzip arm + gate flip + repurposed reject test + determinism pin → T3; real pyarrow gzip dict/plain/nullable fixtures + e2e → T4; pentest (truncation/magic/CM/FEXTRA/FNAME/reserved-btype/bad-dynamic-Huffman/distance-OOB/NLEN/ISIZE/CRC + positive stored/fixed/dynamic/overlap/vanilla locks) → T5; honest gate + intended-change disclosure + SP105-convention record + cumulative USAGE table → T6. All design sections mapped.

**2. Placeholder scan:** `unimplemented!()` appears once as an explicit *instruction marker* in the Task-3 builder skeleton with a directive to mirror `build_snappy_plain_int64_file` byte-for-byte — the implementer fills it from the named existing builder; this is scaffolding with an exact source, not a vague placeholder. The `&/*…bytes here*/` markers are filled from the **documented exact python commands** (independent-authority capture, the established SP101-fixture discipline) — not TBDs. `<BASELINE>/<FINAL>/<DELTA>` are runtime-measured (defined T0/T6). No "handle edge cases"/"similar to" vagueness; every algorithm step and RFC table is specified.

**3. Type consistency:** `gzip::decompress(&[u8], usize) -> Result<Vec<u8>, PqError>`, `gzip::GZIP_MAX_DECOMP: usize`, `crc32(&[u8])->u32`, `inflate(&[u8],usize)->Result<Vec<u8>,PqError>` consistent across T1/T3/T5. `Codec::{Uncompressed,Snappy,Gzip,Other}` matches T2. `page_payload`'s `Cow` return + `meta::Codec` arg unchanged. `PqError::{Bad,Unsupported}`/`PqValue::{I64,Bytes,Null}` match the crate.

Plan is internally consistent and fully covers the OBJ-2c-1 design.
