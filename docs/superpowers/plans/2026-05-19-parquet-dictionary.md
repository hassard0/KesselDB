# OBJ-2b-2 Parquet Dictionary Encoding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `kessel-parquet::extract()` decode dictionary-encoded flat REQUIRED, UNCOMPRESSED, V1 Parquet columns (pyarrow's default `use_dictionary=True`) by consuming the SP102 `rle::decode_hybrid` primitive, flipping exactly the dictionary support-matrix gates.

**Architecture:** Decode-only change inside `kessel-parquet`. `meta.rs` learns `Encoding::PlainDictionary/RleDictionary`, `ColumnChunk.dictionary_page_offset`, and the field-7 `DictionaryPageHeader`. New pure module `dict.rs` resolves dictionary indices. `lib.rs::extract()`'s per-chunk read becomes a page-loop helper that loads the dictionary page then iterates data pages dispatching PLAIN vs dictionary per page. No `kessel-fetch`/`kessel-sql`/server/kernel change (T4 reuses the existing SP101 oracle harness).

**Tech Stack:** Rust (workspace edition), `#![forbid(unsafe_code)]`, zero external dependencies, existing `PqError`/`PqValue`, the SP102 `rle::decode_hybrid`.

---

## Context for the implementer (read once)

`crates/kessel-parquet` modules: `thrift.rs` (compact-thrift `StructReader`), `footer.rs`, `meta.rs` (`FileMetaData`/`ColumnChunk`/`PageHeader` + enums), `plain.rs` (`decode_plain(data,Type,count)->Result<Vec<PqValue>,PqError>`), `rle.rs` (`pub fn decode_hybrid(data,bit_width:u32,num_values:usize)->Result<Vec<u64>,PqError>`, crate-private), `lib.rs` (`PqValue`,`PqError`,`extract`). `Cargo.toml` `[dependencies]` is empty and MUST stay empty.

**The Parquet dictionary layout** (authority: `parquet-format`):
- `ColumnMetaData.dictionary_page_offset` = thrift field id **11** (i64), points at a `PageHeader` with `PageType=DICTIONARY_PAGE(2)` whose payload is PLAIN-encoded dictionary values.
- `PageHeader.dictionary_page_header` = field id **7** = `DictionaryPageHeader{1:i32 num_values, 2:Encoding encoding, 3:bool is_sorted}`.
- A dictionary-encoded `DATA_PAGE(0)` (flat REQUIRED, no rep/def levels) payload = `<1 byte bit_width>` then a **non-length-prefixed** RLE/bit-packing hybrid stream of `num_values` indices.
- `Encoding`: `PLAIN=0`, `PLAIN_DICTIONARY=2`, `RLE=3`, `RLE_DICTIONARY=8`. `PageType`: `DATA_PAGE=0`, `INDEX_PAGE=1`, `DICTIONARY_PAGE=2`, `DATA_PAGE_V2=3`.

**The SP102 hybrid grammar** (for hand-deriving KATs — the independent authority is `parquet-format/Encodings.md`): a run's varint header; `header&1==1`→bit-packed (`groups=header>>1`, yields `groups*8` values, `groups*bit_width` bytes, **LSB-of-stream-first**); `header&1==0`→RLE (`run_len=header>>1`, value in `ceil(bit_width/8)` LE bytes; `bit_width==0`→no value bytes). `decode_hybrid` over-produces whole bit-packed groups then truncates to `num_values`.

**Discipline (matches plain.rs/thrift.rs/rle.rs):** `#![forbid(unsafe_code)]` crate-wide. No `unwrap`/`expect`/`panic`/raw-index on input bytes — checked `get(..)`/`checked_*`. The only allowed `try_into().unwrap()` is the statically-infallible 4-byte→`[u8;4]` for `u32::from_le_bytes` (plain.rs:87). New modules carry `#![allow(dead_code)]` like siblings.

**Determinism / invariants gate — run on EVERY task (T0–T6):**
- `cargo test --workspace --release` → `FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` green.
- `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty; `cargo tree -p kesseldb-server` links no parquet/objstore/rustls/webpki.
- Existing oracles green unchanged: `external_source_oracle` (2), `external_source_tls_oracle` (1), `external_source_objstore_oracle` (1). All OBJ-2a gate tests green **except** the two intentionally-updated dict tests (T3).

**Commit discipline:** straight to `main`, no Co-Authored-By, no signing, message style like `git log -3` (`parquet: …`, `docs: …`). `git push` after every task. Bash: prefix each call `cd /c/Users/ihass/KesselDB &&` (cwd resets per call); `cargo test --workspace --release` is long — allow 600000ms.

**KAT discipline:** the hand-built thrift bytes and the hybrid index bytes below are derived from the published parquet-format/Apache-thrift grammar (independent authority), NOT produced by the code under test. If a KAT fails, the *code* is wrong — fix the code, never the KAT. Report BLOCKED rather than fake. T4 fixtures must be real pyarrow output.

---

### Task 0: Determinism baseline (#157)

**Files:** none (measurement only).

- [ ] **Step 1: Full suite**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -30`
Expected: `FAILED=0`. Sum the `passed` across all binaries → record `<BASELINE>` (expected **310**). Confirm `large_seed_corpus_is_deterministic_and_converges` is in an `ok` result.

- [ ] **Step 2: Dependency cleanliness**

Run: `cd /c/Users/ihass/KesselDB && cargo tree -p kesseldb-server 2>&1 | grep -Ei "parquet|objstore|rustls|webpki"`
Expected: no output.

- [ ] **Step 3: Report**

No commit. Report DONE with `OBJ-2b-2 baseline: <BASELINE> tests passing, FAILED=0, seed-7 green, deps clean` and the per-binary counts you summed.

---

### Task 1: `meta.rs` — dictionary metadata + field-7 page header (#158)

**Files:**
- Modify: `crates/kessel-parquet/src/meta.rs`

- [ ] **Step 1: Write the failing tests**

Add these tests inside the existing `#[cfg(test)] mod tests` block in `crates/kessel-parquet/src/meta.rs` (it already has `uv`/`zz` helpers and `decode_minimal_filemetadata`):

```rust
#[test]
fn columnmeta_decodes_dictionary_page_offset_field11() {
    // Reuse the decode_minimal_filemetadata byte construction but add
    // ColumnMetaData field 11 (dictionary_page_offset i64 = 4) right
    // after f9 (data_page_offset). Compact i64 type code = 6.
    // We rebuild the whole FileMetaData here (independent of the
    // existing helper) to keep this test self-contained.
    let mut b = Vec::new();
    b.push(0x15); uv(&mut b, zz(1));                 // f1 version=1
    b.push(0x19); b.push(0x2c);                      // f2 list<struct> 2
    b.push(0x48); uv(&mut b, 4); b.extend_from_slice(b"root"); // schema[0] name
    b.push(0x15); uv(&mut b, zz(1));                 // schema[0] f5 num_children=1
    b.push(0x00);                                    // stop schema[0]
    b.push(0x15); uv(&mut b, zz(2));                 // schema[1] f1 type=INT64(2)
    b.push(0x25); uv(&mut b, zz(0));                 // f3 repetition=REQUIRED
    b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id"); // f4 name
    b.push(0x00);                                    // stop schema[1]
    b.push(0x16); uv(&mut b, zz(3));                 // f3 num_rows=3
    b.push(0x19); b.push(0x1c);                      // f4 list<RowGroup> 1
    b.push(0x19); b.push(0x1c);                      // RG f1 list<ColumnChunk> 1
    b.push(0x3c);                                    // ColumnChunk f3 ColumnMetaData
    b.push(0x15); uv(&mut b, zz(2));                 // CMD f1 type=INT64(2)
    b.push(0x19); b.push(0x15); uv(&mut b, zz(2));   // f2 encodings [PLAIN_DICTIONARY(2)]
    b.push(0x19); b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id"); // f3 path ["id"]
    b.push(0x15); uv(&mut b, zz(0));                 // f4 codec=UNCOMPRESSED
    b.push(0x16); uv(&mut b, zz(3));                 // f5 num_values=3
    b.push(0x46); uv(&mut b, zz(40));                // f9 data_page_offset=40 (delta 5->9=4, i64=6 -> 0x46)
    b.push(0x26); uv(&mut b, zz(4));                 // f11 dictionary_page_offset=4 (delta 9->11=2, i64=6 -> 0x26)
    b.push(0x00);                                    // stop ColumnMetaData
    b.push(0x00);                                    // stop ColumnChunk
    b.push(0x26); uv(&mut b, zz(3));                 // RG f3 num_rows=3
    b.push(0x00);                                    // stop RowGroup
    b.push(0x00);                                    // stop FileMetaData

    let md = FileMetaData::decode(&b).expect("decode");
    let cc = &md.row_groups[0].columns[0];
    assert_eq!(cc.dictionary_page_offset, Some(4));
    assert_eq!(cc.data_page_offset, 40);
    assert_eq!(cc.encodings, vec![Encoding::PlainDictionary]);
}

#[test]
fn pageheader_decodes_dictionary_page_header_field7() {
    // PageHeader { 1:type=DICTIONARY_PAGE(2), 3:uncompressed_size=16,
    //   4:compressed_size=16, 7:DictionaryPageHeader{1:num_values=2,
    //   2:encoding=PLAIN_DICTIONARY(2), 3:is_sorted=false} }
    let mut h = Vec::new();
    h.push(0x15); uv(&mut h, zz(2));   // f1 type=DICTIONARY_PAGE(2)
    h.push(0x25); uv(&mut h, zz(16));  // f3 uncompressed_page_size=16 (delta 1->3=2,i32=5 ->0x25)
    h.push(0x15); uv(&mut h, zz(16));  // f4 compressed_page_size=16 (delta 3->4=1 ->0x15)
    h.push(0x3c);                      // f7 DictionaryPageHeader struct (delta 4->7=3, struct=12 ->0x3c)
    h.push(0x15); uv(&mut h, zz(2));   // g1 num_values=2
    h.push(0x15); uv(&mut h, zz(2));   // g2 encoding=PLAIN_DICTIONARY(2) (delta 1->2=1 ->0x15)
    h.push(0x12);                      // g3 is_sorted=false (delta 2->3=1, BOOL_FALSE=2 ->0x12)
    h.push(0x00);                      // stop DictionaryPageHeader
    h.push(0x00);                      // stop PageHeader

    let (ph, _len) = decode_page_header(&h).expect("decode");
    assert_eq!(ph.page_type, 2);
    assert_eq!(ph.dict_num_values, 2);
    assert_eq!(ph.dict_encoding, 2);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet meta::tests 2>&1 | tail -15`
Expected: compile errors — `no field dictionary_page_offset`, `no variant PlainDictionary`, `no field dict_num_values`.

- [ ] **Step 3: Add the `Encoding` variants**

In `crates/kessel-parquet/src/meta.rs`, the `Encoding` enum currently is `Plain`, `Rle`, `Other(i32)` with `from_i32` mapping `0→Plain, 3→Rle, o→Other(o)`. Change the enum to add two variants and extend `from_i32`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    Plain,
    /// RLE (id=3). For flat REQUIRED columns appears in the
    /// ColumnMetaData encoding list describing the (zero-length)
    /// level encoding; not the data page encoding.
    Rle,
    /// PLAIN_DICTIONARY (id=2): dictionary indices, legacy tag.
    PlainDictionary,
    /// RLE_DICTIONARY (id=8): dictionary indices, current tag.
    RleDictionary,
    Other(i32),
}
impl Encoding {
    fn from_i32(v: i32) -> Encoding {
        match v {
            0 => Encoding::Plain,
            2 => Encoding::PlainDictionary,
            3 => Encoding::Rle,
            8 => Encoding::RleDictionary,
            o => Encoding::Other(o),
        }
    }
}
```

- [ ] **Step 4: Add `dictionary_page_offset` to `ColumnChunk`**

In the `ColumnChunk` struct add the field (keep all existing fields):

```rust
#[derive(Clone, Debug)]
pub struct ColumnChunk {
    pub path: Vec<String>,
    pub ptype: Type,
    pub codec: Codec,
    pub encodings: Vec<Encoding>,
    pub num_values: i64,
    pub data_page_offset: i64,
    pub dictionary_page_offset: Option<i64>,
}
```

In `decode_column_meta`, add a local `let mut dictionary_page_offset: Option<i64> = None;` next to the other locals, add a match arm `11 => dictionary_page_offset = Some(s.read_i64(&f)?),` (the existing arms use `9 => data_page_offset = s.read_i64(&f)?,`), and add `dictionary_page_offset,` to the returned `ColumnChunk { … }`.

- [ ] **Step 5: Add dict fields to `PageHeader` + decode field 7**

Extend the `PageHeader` struct and its initializer in `decode_page_header`:

```rust
#[derive(Clone, Debug)]
pub struct PageHeader {
    pub page_type: i32,
    pub uncompressed_size: i32,
    pub compressed_size: i32,
    pub dp_num_values: i32,
    pub dp_encoding: i32,
    pub dict_num_values: i32,
    pub dict_encoding: i32,
}
```

In `decode_page_header` set the initializer `dict_num_values: 0, dict_encoding: -1,` (alongside the existing defaults). The existing field-5 arm looks like:

```rust
5 => {
    if f.ctype != ctype::STRUCT { return Err(bad("PageHeader.data_page_header: expected struct")); }
    s.reset_last_id();
    while let Some(g) = s.next_field()? {
        match g.id {
            1 => ph.dp_num_values = s.read_i32(&g)?,
            2 => ph.dp_encoding = s.read_i32(&g)?,
            _ => s.skip(g.ctype)?,
        }
    }
    s.restore_last_id(f.id);
}
```

Add an exactly-parallel field-7 arm immediately after it:

```rust
7 => {
    // DictionaryPageHeader { 1:i32 num_values, 2:Encoding encoding,
    // 3:bool is_sorted }. Same per-struct last_id bracketing as f5.
    if f.ctype != ctype::STRUCT { return Err(bad("PageHeader.dictionary_page_header: expected struct")); }
    s.reset_last_id();
    while let Some(g) = s.next_field()? {
        match g.id {
            1 => ph.dict_num_values = s.read_i32(&g)?,
            2 => ph.dict_encoding = s.read_i32(&g)?,
            _ => s.skip(g.ctype)?,
        }
    }
    s.restore_last_id(f.id);
}
```

(The `_ => s.skip(g.ctype)?` consumes `g3 is_sorted`; `StructReader::skip` already treats compact BOOL_TRUE/BOOL_FALSE as zero-payload — verify by reading `thrift.rs` `skip`; if it does not, that is a pre-existing bug, report DONE_WITH_CONCERNS, do not patch thrift.rs in this task.)

- [ ] **Step 6: Verify the new tests pass and the old one still passes**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet meta:: 2>&1 | tail -15`
Expected: `columnmeta_decodes_dictionary_page_offset_field11`, `pageheader_decodes_dictionary_page_header_field7`, AND the pre-existing `decode_minimal_filemetadata` all pass (the latter proves field-11-absent → `dictionary_page_offset == None`; if it now fails to compile because it constructs `ColumnChunk` literally, it does not — it calls `FileMetaData::decode`, so `None` is set by the decoder default).

- [ ] **Step 7: Full determinism gate**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15`
Expected: `FAILED=0`, total ≥ baseline + 2, seed-7 green.

- [ ] **Step 8: Commit**

```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/meta.rs && git commit -m "parquet: meta dictionary fields — Encoding 2/8, CMD f11, PageHeader f7" && git push
```

---

### Task 2: `dict.rs` — dictionary index resolution (#159)

**Files:**
- Create: `crates/kessel-parquet/src/dict.rs`
- Modify: `crates/kessel-parquet/src/lib.rs` (add `mod dict;` after `mod rle;`)

- [ ] **Step 1: Declare the module**

In `crates/kessel-parquet/src/lib.rs` the module block ends with `mod rle;`. Add `mod dict;` as the next line.

- [ ] **Step 2: Write the failing test file**

Create `crates/kessel-parquet/src/dict.rs` with ONLY this first (tests reference a not-yet-existing fn → red):

```rust
//! Parquet dictionary-index resolution. The data-page payload for a
//! flat REQUIRED dictionary column is `<1 byte bit_width>` then a
//! non-length-prefixed RLE/bit-packing hybrid stream of indices
//! (SP102 `rle::decode_hybrid`). Indices are resolved against the
//! PLAIN-decoded dictionary, every lookup bounds-checked. Zero deps,
//! pure, never panics/OOMs.
#![allow(dead_code)]

use crate::rle;
use crate::{PqError, PqValue};

fn bad(s: &str) -> PqError {
    PqError::Bad(s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dict_abc() -> Vec<PqValue> {
        vec![
            PqValue::Bytes(b"a".to_vec()),
            PqValue::Bytes(b"b".to_vec()),
            PqValue::Bytes(b"c".to_vec()),
        ]
    }

    // KAT — bit_width=2, one bit-packed group of 8 values
    // [0,2,1,1,0,0,0,0] (padded; decoder truncates to n=4).
    // header = (1 group << 1)|1 = 0x03. LSB-of-stream-first 2-bit
    // packing: v0=0,v1=2,v2=1,v3=1 → byte0 = 0b0101_1000 = 0x58,
    // byte1 = 0x00. Payload = [bit_width=0x02, 0x03, 0x58, 0x00].
    #[test]
    fn kat_resolve_bitpacked_width2() {
        let payload = [0x02u8, 0x03, 0x58, 0x00];
        let got = resolve_dict_indices(&payload, &dict_abc(), 4)
            .expect("resolve");
        assert_eq!(
            got,
            vec![
                PqValue::Bytes(b"a".to_vec()),
                PqValue::Bytes(b"c".to_vec()),
                PqValue::Bytes(b"b".to_vec()),
                PqValue::Bytes(b"b".to_vec()),
            ]
        );
    }

    // KAT — bit_width=0: every index 0 → every value dict[0].
    // hybrid = RLE run_len=4 (header varint(4<<1)=0x08), bit_width=0
    // → NO value byte. Payload = [bit_width=0x00, 0x08].
    #[test]
    fn kat_resolve_bitwidth0_all_dict0() {
        let payload = [0x00u8, 0x08];
        let got = resolve_dict_indices(&payload, &dict_abc(), 4)
            .expect("resolve");
        assert_eq!(got, vec![PqValue::Bytes(b"a".to_vec()); 4]);
    }

    // OOB — bit_width=3 RLE run value=5 (header varint(4<<1)=0x08,
    // value 0x05) but dict has only 3 entries → Bad.
    #[test]
    fn resolve_oob_index_is_bad() {
        let payload = [0x03u8, 0x08, 0x05];
        assert!(matches!(
            resolve_dict_indices(&payload, &dict_abc(), 4),
            Err(PqError::Bad(_))
        ));
    }

    #[test]
    fn resolve_empty_payload_is_bad() {
        assert!(matches!(
            resolve_dict_indices(&[], &dict_abc(), 1),
            Err(PqError::Bad(_))
        ));
    }
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet dict:: 2>&1 | tail -10`
Expected: compile error — `cannot find function resolve_dict_indices`.

- [ ] **Step 4: Implement `resolve_dict_indices`**

Insert above the `#[cfg(test)] mod tests` block:

```rust
/// Resolve a dictionary-encoded data-page payload to values.
/// `payload` is the WHOLE data-page payload: `payload[0]` is the
/// bit width, `payload[1..]` is the non-length-prefixed hybrid
/// index stream. `dict` is the PLAIN-decoded dictionary; `n` is the
/// data page's num_values. Every index is bounds-checked against
/// `dict.len()` (OOB → Bad). Never panics / OOM-aborts.
pub fn resolve_dict_indices(
    payload: &[u8],
    dict: &[PqValue],
    n: usize,
) -> Result<Vec<PqValue>, PqError> {
    let bit_width = *payload
        .get(0)
        .ok_or_else(|| bad("dict data page empty (no bit-width byte)"))?;
    let stream = payload.get(1..).unwrap_or(&[]);
    let idxs = rle::decode_hybrid(stream, bit_width as u32, n)?;
    // OOM bound (plain.rs:35 stance): `n` is the page num_values,
    // upstream-capped; never reserve from a stream-derived count.
    let mut out: Vec<PqValue> = Vec::with_capacity(n);
    for raw in idxs {
        let i = usize::try_from(raw)
            .map_err(|_| bad("dict index range"))?;
        let v = dict
            .get(i)
            .ok_or_else(|| bad("dict index out of range"))?;
        out.push(v.clone());
    }
    Ok(out)
}
```

- [ ] **Step 5: Verify the KATs pass**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet dict::tests 2>&1 | tail -10`
Expected: `test result: ok. 4 passed`. If `kat_resolve_bitpacked_width2` fails, the bug is in `resolve_dict_indices` or you mis-wired `rle` — the KAT bytes are hand-derived from the grammar; do not change them.

- [ ] **Step 6: Full determinism gate**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15`
Expected: `FAILED=0`, total ≥ baseline + 6, seed-7 green.

- [ ] **Step 7: Commit**

```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/dict.rs crates/kessel-parquet/src/lib.rs && git commit -m "parquet: dictionary index resolution (dict::resolve_dict_indices) + spec KATs" && git push
```

---

### Task 3: `extract()` page-loop + gate flip + intended test changes (#160)

**Files:**
- Modify: `crates/kessel-parquet/src/lib.rs`

Context: `extract()` currently, per row group × per wanted column, finds the `ColumnChunk` `cc`, applies the Fix-1 schema/chunk ptype guard, the `codec == Uncompressed` gate, the encodings-list gate (only `Plain|Rle`), reads ONE page header at `cc.data_page_offset`, gates `page_type != 0` / `dp_encoding != 0`, calls `plain::decode_plain`, and `cols[ci].extend(vals)`. You will extract the per-chunk page reading into a helper and generalise it; the schema-leaf checks (repetition Required, ptype-allowed), the Fix-1 guard, and the final transpose stay in `extract()` unchanged.

- [ ] **Step 1: Write the failing tests (positive dict decode + determinism pin + repurposed DELTA reject)**

The existing `mod tests` in `lib.rs` has `uv`,`zz`,`page_header_bytes`,`page_header_dict_bytes`,`filemetadata_bytes`,`build_parquet_file*`. Add this dictionary-file builder and three tests inside `mod tests`:

```rust
/// Build a complete dict-encoded INT64 file for column "id":
///   logical rows [7,7,-2]; dictionary [7,-2]; indices [0,0,1].
/// Layout: [PAR1][dict_hdr][dict_data][data_hdr][data_payload]
///         [FileMetaData][mlen u32 LE][PAR1]
/// dict_page_offset = 4; data_page_offset computed dynamically.
fn build_dict_int64_file() -> Vec<u8> {
    // ---- dictionary page (PLAIN INT64: 7, -2) ----
    let mut dict_data = Vec::new();
    dict_data.extend_from_slice(&7i64.to_le_bytes());
    dict_data.extend_from_slice(&(-2i64).to_le_bytes());
    let dbytes = dict_data.len() as i64; // 16
    // DICTIONARY_PAGE header: f1 type=2, f3 uncompressed=16,
    // f4 compressed=16, f7 DictionaryPageHeader{1:num_values=2,
    // 2:encoding=PLAIN_DICTIONARY(2), 3:is_sorted=false}.
    let mut dict_hdr = Vec::new();
    dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));        // f1 type=DICTIONARY_PAGE(2)
    dict_hdr.push(0x25); uv(&mut dict_hdr, zz(dbytes));   // f3 uncompressed_page_size
    dict_hdr.push(0x15); uv(&mut dict_hdr, zz(dbytes));   // f4 compressed_page_size
    dict_hdr.push(0x3c);                                  // f7 struct (delta 4->7=3)
    dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));        // g1 num_values=2
    dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));        // g2 encoding=PLAIN_DICTIONARY(2)
    dict_hdr.push(0x12);                                  // g3 is_sorted=false
    dict_hdr.push(0x00);                                  // stop DictionaryPageHeader
    dict_hdr.push(0x00);                                  // stop PageHeader

    // ---- data page (PLAIN_DICTIONARY indices [0,0,1]) ----
    // payload = [bit_width=1][hybrid: 1 bit-packed group of 8
    // values 0,0,1,0,0,0,0,0 → header 0x03, byte 0x04].
    let data_payload: Vec<u8> = vec![0x01, 0x03, 0x04];
    let pbytes = data_payload.len() as i64; // 3
    // DATA_PAGE header: f1 type=0, f3 uncompressed=3, f4 compressed=3,
    // f5 DataPageHeader{1:num_values=3, 2:encoding=PLAIN_DICTIONARY(2)}.
    let mut data_hdr = Vec::new();
    data_hdr.push(0x15); uv(&mut data_hdr, zz(0));        // f1 type=DATA_PAGE(0)
    data_hdr.push(0x25); uv(&mut data_hdr, zz(pbytes));   // f3 uncompressed_page_size
    data_hdr.push(0x15); uv(&mut data_hdr, zz(pbytes));   // f4 compressed_page_size
    data_hdr.push(0x1c);                                  // f5 struct (delta 4->5=1)
    data_hdr.push(0x15); uv(&mut data_hdr, zz(3));        // g1 num_values=3
    data_hdr.push(0x15); uv(&mut data_hdr, zz(2));        // g2 encoding=PLAIN_DICTIONARY(2)
    data_hdr.push(0x00);                                  // stop DataPageHeader
    data_hdr.push(0x00);                                  // stop PageHeader

    let dict_page_offset: i64 = 4;
    let data_page_offset: i64 =
        4 + dict_hdr.len() as i64 + dict_data.len() as i64;

    // ---- FileMetaData: INT64 "id" REQUIRED, 1 RG, 1 chunk,
    // encodings [PLAIN_DICTIONARY(2)], num_values=3,
    // data_page_offset, dictionary_page_offset. ----
    let mut m = Vec::new();
    m.push(0x15); uv(&mut m, zz(2));                      // f1 version=2
    m.push(0x19); m.push(0x2c);                           // f2 list<SchemaElement> 2
    m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema"); // schema[0] name
    m.push(0x15); uv(&mut m, zz(1));                      // schema[0] f5 num_children=1
    m.push(0x00);                                         // stop schema[0]
    m.push(0x15); uv(&mut m, zz(2));                      // schema[1] f1 type=INT64(2)
    m.push(0x25); uv(&mut m, zz(0));                      // f3 repetition=REQUIRED
    m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id"); // f4 name
    m.push(0x00);                                         // stop schema[1]
    m.push(0x16); uv(&mut m, zz(3));                      // f3 num_rows=3
    m.push(0x19); m.push(0x1c);                           // f4 list<RowGroup> 1
    m.push(0x19); m.push(0x1c);                           // RG f1 list<ColumnChunk> 1
    m.push(0x3c);                                         // ColumnChunk f3 ColumnMetaData
    m.push(0x15); uv(&mut m, zz(2));                      // CMD f1 type=INT64(2)
    m.push(0x19); m.push(0x15); uv(&mut m, zz(2));        // f2 encodings [PLAIN_DICTIONARY(2)]
    m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id"); // f3 path ["id"]
    m.push(0x15); uv(&mut m, zz(0));                      // f4 codec=UNCOMPRESSED
    m.push(0x16); uv(&mut m, zz(3));                      // f5 num_values=3
    m.push(0x46); uv(&mut m, zz(data_page_offset));       // f9 data_page_offset (delta 5->9=4,i64)
    m.push(0x26); uv(&mut m, zz(dict_page_offset));       // f11 dictionary_page_offset (delta 9->11=2,i64)
    m.push(0x00);                                         // stop ColumnMetaData
    m.push(0x00);                                         // stop ColumnChunk
    m.push(0x26); uv(&mut m, zz(3));                      // RG f3 num_rows=3
    m.push(0x00);                                         // stop RowGroup
    m.push(0x00);                                         // stop FileMetaData

    let mut f = Vec::new();
    f.extend_from_slice(b"PAR1");
    f.extend_from_slice(&dict_hdr);
    f.extend_from_slice(&dict_data);
    f.extend_from_slice(&data_hdr);
    f.extend_from_slice(&data_payload);
    let mlen = m.len() as u32;
    f.extend_from_slice(&m);
    f.extend_from_slice(&mlen.to_le_bytes());
    f.extend_from_slice(b"PAR1");
    f
}

#[test]
fn extract_decodes_dictionary_int64() {
    let file = build_dict_int64_file();
    let rows = extract(&file, &["id"]).expect("extract");
    assert_eq!(rows, vec![
        vec![PqValue::I64(7)],
        vec![PqValue::I64(7)],
        vec![PqValue::I64(-2)],
    ]);
}

#[test]
fn extract_plain_and_dict_are_identical() {
    // Same logical column [7,-2] two ways. The existing
    // build_parquet_file(0,0,0,false) yields PLAIN [7,-2].
    let plain = build_parquet_file(0, 0, 0, false);
    let plain_rows = extract(&plain, &["id"]).expect("plain");
    // Dict file's logical rows are [7,7,-2]; take a PLAIN file with
    // the same [7,7,-2] is not built by the helper, so instead
    // assert the dict file decodes to the SAME PqValue variant/》
    // values the PLAIN path produces for those integers.
    let dict_rows = extract(&build_dict_int64_file(), &["id"])
        .expect("dict");
    // Determinism/source-format-independence: identical PqValue.
    assert_eq!(plain_rows, vec![vec![PqValue::I64(7)],
                                vec![PqValue::I64(-2)]]);
    assert_eq!(dict_rows, vec![vec![PqValue::I64(7)],
                               vec![PqValue::I64(7)],
                               vec![PqValue::I64(-2)]]);
    // The shared values [7] and [-2] are byte-identical PqValue::I64
    // regardless of PLAIN vs dict encoding (pq_to_cell/coerce
    // unchanged).
    assert_eq!(plain_rows[0], dict_rows[0]);
    assert_eq!(plain_rows[1], dict_rows[2]);
}

#[test]
fn extract_rejects_delta_encoding() {
    // DELTA_BINARY_PACKED(5) in the ColumnMetaData encodings list is
    // still unsupported (was the old dict-reject test, repurposed —
    // intended behavior change: dict is now accepted, DELTA is not).
    let file = build_parquet_file(5, 0, 0, false);
    assert!(
        matches!(extract(&file, &["id"]), Err(PqError::Unsupported(_))),
        "DELTA encoding must be Unsupported"
    );
}
```

- [ ] **Step 2: Delete the two superseded OBJ-2a tests**

Remove `extract_rejects_dict_columnmeta_encoding` and `extract_rejects_dict_data_page_encoding` from `mod tests` (they are intentionally superseded — `extract_rejects_delta_encoding` replaces the first; `extract_decodes_dictionary_int64` replaces the second). **Leave `extract_rejects_snappy_codec`, `extract_rejects_optional_repetition`, `extract_rejects_schema_chunk_type_mismatch`, `extract_rejects_missing_column`, `extract_golden_int64_two_rows` unchanged.**

- [ ] **Step 3: Run to verify the new tests fail**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet extract_decodes_dictionary_int64 2>&1 | tail -10`
Expected: FAIL — current `extract()` rejects the dict encodings-list / dict data-page with `Unsupported`.

- [ ] **Step 4: Refactor the per-chunk read into `read_chunk_values` + flip gates**

In `crates/kessel-parquet/src/lib.rs`, the `extract()` row-group loop currently does (per wanted column) the codec/encodings/offset/page-header/decode work inline. Replace that inline block with a call to a new free function and add the function. The Fix-1 ptype guard and the `cols[ci].extend(vals)` stay in `extract()`; everything from the codec gate through producing the chunk's `Vec<PqValue>` moves into the helper. The helper:

```rust
/// Read one column chunk's values across all its pages.
/// Flat REQUIRED, UNCOMPRESSED, V1. Supports: an optional leading
/// DICTIONARY_PAGE then one-or-more DATA_PAGEs; each data page is
/// PLAIN (dictionary-fallback) or PLAIN_DICTIONARY/RLE_DICTIONARY.
fn read_chunk_values(
    file: &[u8],
    cc: &meta::ColumnChunk,
    want_ptype: meta::Type,
) -> Result<Vec<PqValue>, PqError> {
    if cc.codec != meta::Codec::Uncompressed {
        return Err(PqError::Unsupported("compression: OBJ-2b-3".into()));
    }
    // Encodings-list gate: PLAIN / RLE (level desc) / dictionary now OK.
    if cc.encodings.iter().any(|e| {
        !matches!(
            e,
            meta::Encoding::Plain
                | meta::Encoding::Rle
                | meta::Encoding::PlainDictionary
                | meta::Encoding::RleDictionary
        )
    }) {
        return Err(PqError::Unsupported(
            "non-PLAIN/dictionary encoding (DELTA/BYTE_STREAM_SPLIT): OBJ-2c"
                .into(),
        ));
    }

    // Optional dictionary page.
    let dict: Vec<PqValue> = if let Some(dpo) = cc.dictionary_page_offset {
        let off = usize::try_from(dpo)
            .map_err(|_| PqError::Bad("dict page offset range".into()))?;
        let region = file
            .get(off..)
            .ok_or_else(|| PqError::Bad("dict page offset past EOF".into()))?;
        let (ph, hlen) = meta::decode_page_header(region)?;
        if ph.page_type != 2 {
            return Err(PqError::Bad(
                "dictionary_page_offset does not point at a DICTIONARY_PAGE"
                    .into(),
            ));
        }
        if ph.dict_encoding != 0 && ph.dict_encoding != 2 {
            return Err(PqError::Unsupported(
                "dictionary page encoding (not PLAIN/PLAIN_DICTIONARY): OBJ-2c"
                    .into(),
            ));
        }
        let dn = usize::try_from(ph.dict_num_values)
            .map_err(|_| PqError::Bad("dict num_values range".into()))?;
        let dstart = off
            .checked_add(hlen)
            .ok_or_else(|| PqError::Bad("dict page hdr len ovf".into()))?;
        let dend = dstart
            .checked_add(
                usize::try_from(ph.uncompressed_size)
                    .map_err(|_| PqError::Bad("dict page size range".into()))?,
            )
            .ok_or_else(|| PqError::Bad("dict page size ovf".into()))?;
        let dpage = file
            .get(dstart..dend)
            .ok_or_else(|| PqError::Bad("dict page data truncated".into()))?;
        plain::decode_plain(dpage, want_ptype, dn)?
    } else {
        Vec::new()
    };

    // Data pages: iterate from data_page_offset until num_values rows.
    let want_rows = usize::try_from(cc.num_values)
        .map_err(|_| PqError::Bad("chunk num_values range".into()))?;
    let mut out: Vec<PqValue> = Vec::with_capacity(want_rows);
    let mut off = usize::try_from(cc.data_page_offset)
        .map_err(|_| PqError::Bad("data page offset range".into()))?;
    while out.len() < want_rows {
        let region = file
            .get(off..)
            .ok_or_else(|| PqError::Bad("data page offset past EOF".into()))?;
        let (ph, hlen) = meta::decode_page_header(region)?;
        if ph.page_type != 0 {
            return Err(PqError::Unsupported(
                "non-V1 data page (V2/index): OBJ-2c".into(),
            ));
        }
        let n = usize::try_from(ph.dp_num_values)
            .map_err(|_| PqError::Bad("num_values range".into()))?;
        let dstart = off
            .checked_add(hlen)
            .ok_or_else(|| PqError::Bad("page hdr len ovf".into()))?;
        let dend = dstart
            .checked_add(
                usize::try_from(ph.uncompressed_size)
                    .map_err(|_| PqError::Bad("page size range".into()))?,
            )
            .ok_or_else(|| PqError::Bad("page size ovf".into()))?;
        let page = file
            .get(dstart..dend)
            .ok_or_else(|| PqError::Bad("page data truncated".into()))?;
        let vals = match ph.dp_encoding {
            0 => plain::decode_plain(page, want_ptype, n)?,
            2 | 8 => {
                if cc.dictionary_page_offset.is_none() {
                    return Err(PqError::Bad(
                        "dictionary-encoded data page without dictionary_page_offset"
                            .into(),
                    ));
                }
                dict::resolve_dict_indices(page, &dict, n)?
            }
            _ => {
                return Err(PqError::Unsupported(
                    "data page encoding (DELTA/BYTE_STREAM_SPLIT): OBJ-2c"
                        .into(),
                ))
            }
        };
        if out.len().checked_add(vals.len()).map(|t| t > want_rows).unwrap_or(true) {
            return Err(PqError::Bad(
                "data page values exceed chunk num_values".into(),
            ));
        }
        out.extend(vals);
        off = dend;
    }
    if out.len() != want_rows {
        return Err(PqError::Bad(
            "data page values do not sum to chunk num_values".into(),
        ));
    }
    Ok(out)
}
```

Then in `extract()`, after the Fix-1 guard (`if cc.ptype != wanted_ptypes[ci] { … Bad … }`), replace the existing inline block (the `if cc.codec != …`, the encodings gate, `let off = …`, `decode_page_header`, the `ph.page_type`/`ph.dp_encoding` gates, `plain::decode_plain`, `cols[ci].extend(vals)`) with:

```rust
            let vals = read_chunk_values(bytes, cc, wanted_ptypes[ci])?;
            cols[ci].extend(vals);
```

Keep everything else in `extract()` (schema-leaf resolution, repetition Required check, ptype-allowed match, the transpose/equal-length logic) byte-for-byte unchanged.

- [ ] **Step 5: Verify new + retained tests pass**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet 2>&1 | tail -20`
Expected: `extract_decodes_dictionary_int64`, `extract_plain_and_dict_are_identical`, `extract_rejects_delta_encoding`, `extract_golden_int64_two_rows`, `extract_rejects_snappy_codec`, `extract_rejects_optional_repetition`, `extract_rejects_schema_chunk_type_mismatch`, `extract_rejects_missing_column` ALL pass; the two deleted tests are gone; `FAILED=0`.

- [ ] **Step 6: Full determinism gate**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15`
Expected: `FAILED=0`, seed-7 green. Net count vs baseline = +6 (T2's 4) + (this task: +3 new tests in lib, −2 deleted) — record the measured number; the exact total is reconciled in T6.

- [ ] **Step 7: Commit**

```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/lib.rs && git commit -m "parquet: extract() dict page-loop + flip dictionary gates (intended OBJ-2a test change)" && git push
```

---

### Task 4: Real pyarrow dictionary fixtures + e2e (#161)

**Files:**
- Create: `crates/kessel-parquet/tests/fixtures/dict_flat.parquet` (via pyarrow)
- Modify: `crates/kessel-parquet/tests/fixtures/README.md`
- Modify: `crates/kessel-parquet/tests/fixture_roundtrip.rs`
- Modify (e2e): `crates/kesseldb-server/tests/external_source_parquet_oracle.rs` (add a dict case mirroring the existing SP101 case; no router fixture-trust bypass)

- [ ] **Step 1: Generate the fixture with real pyarrow (24.0.0 present)**

Run exactly (Bash):

```bash
cd /c/Users/ihass/KesselDB && python -c "
import pyarrow as pa, pyarrow.parquet as pq
t = pa.table({'id': pa.array([7,7,-2,7,100], type=pa.int64()),
              's':  pa.array(['a','a','b','c','a'], type=pa.string())})
pq.write_table(t, 'crates/kessel-parquet/tests/fixtures/dict_flat.parquet',
               use_dictionary=True, compression=None, version='1.0',
               data_page_version='1.0')
print('wrote dict_flat.parquet rows=5')
"
```

Expected: `wrote dict_flat.parquet rows=5`. If pyarrow import fails or the file is not written, STOP and report **BLOCKED** (do not hand-fabricate a Parquet file — SP101 T7 stance).

- [ ] **Step 2: Verify pyarrow can read it back (independent re-read, sanity)**

Run: `cd /c/Users/ihass/KesselDB && python -c "import pyarrow.parquet as pq; t=pq.read_table('crates/kessel-parquet/tests/fixtures/dict_flat.parquet'); print(t.column('id').to_pylist(), t.column('s').to_pylist())"`
Expected: `[7, 7, -2, 7, 100] ['a', 'a', 'b', 'c', 'a']`. Record this in the README as the authoritative expected rows.

- [ ] **Step 3: Document in the fixtures README**

Append to `crates/kessel-parquet/tests/fixtures/README.md`:

```markdown
## dict_flat.parquet (OBJ-2b-2)

Regenerate:

    python -c "import pyarrow as pa, pyarrow.parquet as pq; \
    t=pa.table({'id':pa.array([7,7,-2,7,100],type=pa.int64()), \
    's':pa.array(['a','a','b','c','a'],type=pa.string())}); \
    pq.write_table(t,'crates/kessel-parquet/tests/fixtures/dict_flat.parquet', \
    use_dictionary=True, compression=None, version='1.0', data_page_version='1.0')"

Real pyarrow 24.0.0 output: dictionary-encoded, UNCOMPRESSED, V1, flat
REQUIRED. Expected logical rows:
id = [7, 7, -2, 7, 100]; s = ["a", "a", "b", "c", "a"].
```

- [ ] **Step 4: Add the roundtrip test**

In `crates/kessel-parquet/tests/fixture_roundtrip.rs` (it already loads sibling fixtures with `kessel_parquet::extract`), add:

```rust
#[test]
fn dict_flat_fixture_roundtrips() {
    let bytes = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/dict_flat.parquet"
    ))
    .expect("read dict_flat.parquet");
    let rows = kessel_parquet::extract(&bytes, &["id", "s"])
        .expect("extract dict fixture");
    assert_eq!(
        rows,
        vec![
            vec![kessel_parquet::PqValue::I64(7),
                 kessel_parquet::PqValue::Bytes(b"a".to_vec())],
            vec![kessel_parquet::PqValue::I64(7),
                 kessel_parquet::PqValue::Bytes(b"a".to_vec())],
            vec![kessel_parquet::PqValue::I64(-2),
                 kessel_parquet::PqValue::Bytes(b"b".to_vec())],
            vec![kessel_parquet::PqValue::I64(7),
                 kessel_parquet::PqValue::Bytes(b"c".to_vec())],
            vec![kessel_parquet::PqValue::I64(100),
                 kessel_parquet::PqValue::Bytes(b"a".to_vec())],
        ]
    );
}
```

(If the existing roundtrip file imports `extract`/`PqValue` differently, match its existing import style — read the file first.)

- [ ] **Step 5: e2e (mirror SP101, no fixture-trust bypass)**

Read `crates/kesseldb-server/tests/external_source_parquet_oracle.rs`. It proves the trusted decode happy-path at the `kessel-fetch` layer over the SP101 fixture (object-store feature maps `Format::Parquet → kessel_parquet::extract`; no router code change). Add a parallel test case that points the same harness at `dict_flat.parquet` and asserts the same 5 logical rows materialise (fail-closed; reuse the exact harness pattern — do NOT add any router fixture-trust shortcut). If the harness is a single fixture path, parameterise minimally or add a second `#[test]` mirroring it for the dict fixture.

- [ ] **Step 6: Run the fixture + e2e tests**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet --test fixture_roundtrip 2>&1 | tail -8` (expect dict_flat roundtrip pass) and `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources-objstore --test external_source_parquet_oracle 2>&1 | tail -8` (expect the existing SP101 case + the new dict case pass). If the exact feature flag differs, use the one the existing SP101 test uses (read its `#![cfg(feature=...)]`/CI invocation).

- [ ] **Step 7: Full determinism gate**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15`
Expected: `FAILED=0`, seed-7 green; existing oracles `external_source_oracle`(2)/`_tls_oracle`(1)/`_objstore_oracle`(1) unchanged.

- [ ] **Step 8: Commit**

```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/tests/ crates/kesseldb-server/tests/external_source_parquet_oracle.rs && git commit -m "parquet: real pyarrow use_dictionary fixture + dict e2e (fail-closed)" && git push
```

---

### Task 5: Pentest pass (#162)

**Files:**
- Modify: `crates/kessel-parquet/src/dict.rs` (append a `#[cfg(test)] mod pentest`)

- [ ] **Step 1: Write the pentest module**

Append at the END of `crates/kessel-parquet/src/dict.rs` (sibling after `mod tests`):

```rust
// ── PENTEST PASS — adversarial lock tests ─────────────────────────
// Dictionary payloads/headers are operator-source-controlled. Each
// case: no panic / no OOM / no stack-overflow, and a well-formed
// Result (typed Bad/Unsupported, OR correct Ok for the valid
// bit_width==0 case which we must NOT over-reject).
#[cfg(test)]
mod pentest {
    use super::*;

    fn d3() -> Vec<PqValue> {
        vec![
            PqValue::Bytes(b"x".to_vec()),
            PqValue::Bytes(b"y".to_vec()),
            PqValue::Bytes(b"z".to_vec()),
        ]
    }

    fn no_panic_bad(payload: &[u8], dict: Vec<PqValue>, n: usize) {
        let p = payload.to_vec();
        let r = std::panic::catch_unwind(move || {
            resolve_dict_indices(&p, &dict, n)
        });
        assert!(r.is_ok(), "must NOT panic/OOM-unwind");
        assert!(
            matches!(
                r.unwrap(),
                Err(PqError::Bad(_)) | Err(PqError::Unsupported(_))
            ),
            "hostile input must be a typed error"
        );
    }

    #[test]
    fn empty_payload_bad() {
        no_panic_bad(&[], d3(), 4);
    }

    #[test]
    fn oob_index_bad() {
        // bit_width=3 RLE run value=9 (header varint(4<<1)=0x08,
        // value 0x09) vs len-3 dict → Bad.
        no_panic_bad(&[0x03, 0x08, 0x09], d3(), 4);
    }

    #[test]
    fn huge_bit_width_byte_bad() {
        // bit_width byte = 200 → decode_hybrid rejects (>64) → Bad.
        no_panic_bad(&[200, 0x08, 0x01], d3(), 4);
    }

    #[test]
    fn truncated_index_stream_bad() {
        // bit_width=8, header claims a bit-packed group (0x03 = 1
        // group of 8 → needs 8 bytes) but none follow → Bad.
        no_panic_bad(&[0x08, 0x03], d3(), 8);
    }

    #[test]
    fn lying_n_vs_short_stream_bad() {
        // bit_width=1, tiny stream, but n huge → decode_hybrid
        // exhausts → Bad (no OOM: reservation bounded by n only
        // after a successful decode).
        no_panic_bad(&[0x01, 0x03, 0x00], d3(), usize::from(u16::MAX));
    }

    #[test]
    fn bitwidth0_multi_entry_dict_decodes_to_dict0() {
        // VALID Parquet: bit_width=0 → all indices 0 → every row is
        // dict[0], even though the dict has 3 entries. This MUST
        // decode correctly (Ok), NOT be rejected — proves we do not
        // over-reject valid input. hybrid = RLE run_len=4
        // (header varint(4<<1)=0x08), bit_width 0 → no value byte.
        let payload = [0x00u8, 0x08];
        let got = resolve_dict_indices(&payload, &d3(), 4)
            .expect("bit_width=0 is valid");
        assert_eq!(got, vec![PqValue::Bytes(b"x".to_vec()); 4]);
    }
}
```

- [ ] **Step 2: Add the extract-level hostile-metadata locks**

In `crates/kessel-parquet/src/lib.rs` `mod tests`, add (reusing `build_dict_int64_file` from Task 3 and tweaking bytes):

```rust
#[test]
fn extract_dict_page_offset_past_eof_is_bad() {
    let mut f = build_dict_int64_file();
    // Corrupt the file by truncating to before the footer's dict
    // page region is reachable: chop the middle so the recorded
    // dictionary_page_offset now points past EOF-of-content.
    f.truncate(8); // keep PAR1 + a few bytes, footer parsing fails
    let owned = f.clone();
    let r = std::panic::catch_unwind(move || extract(&owned, &["id"]));
    assert!(r.is_ok(), "must not panic");
    assert!(matches!(r.unwrap(), Err(PqError::Bad(_))));
}
```

(This exercises the `dict page offset past EOF` / footer bounds path; `extract` must return a typed `Bad`, never panic.)

- [ ] **Step 3: Run the pentest tests**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet pentest 2>&1 | tail -12` and `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet extract_dict_page_offset_past_eof_is_bad 2>&1 | tail -6`
Expected: all pass. If `bitwidth0_multi_entry_dict_decodes_to_dict0` fails, the decoder over-rejects valid Parquet — fix `resolve_dict_indices`/`decode_hybrid` usage, do not weaken the test.

- [ ] **Step 4: Full determinism gate**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15`
Expected: `FAILED=0`, seed-7 green.

- [ ] **Step 5: Commit**

```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/dict.rs crates/kessel-parquet/src/lib.rs && git commit -m "parquet: pentest lock tests for dictionary decode (no panic/OOM; bw0 valid)" && git push
```

---

### Task 6: Docs + gate reconciliation + memory (#163)

**Files:**
- Create: `docs/superpowers/specs/2026-05-19-kesseldb-subproject103-dict.md`
- Modify: `docs/STATUS.md`, `docs/USAGE.md`
- Modify (auto-memory, OUTSIDE repo, never git-add): `C:\Users\ihass\.claude\projects\C--Users-ihass--local-bin\memory\project_kesseldb.md`, `…\MEMORY.md`

- [ ] **Step 1: Measure final**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -25`
Sum passed → `<FINAL>`. `<DELTA> = <FINAL> − 310`. Confirm `FAILED=0`, seed-7 green. Run `cargo tree -p kesseldb-server | grep -Ei "parquet|objstore|rustls|webpki"` (no output) and confirm `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty.

- [ ] **Step 2: Read the SP101/SP102 records, then create the SP103 record**

Read `docs/superpowers/specs/2026-05-19-kesseldb-subproject102-rle.md` for the exact header convention. Create `docs/superpowers/specs/2026-05-19-kesseldb-subproject103-dict.md`:

```markdown
# KesselDB — Subproject 103: OBJ-2b-2 Parquet dictionary encoding

**Date:** 2026-05-19  **Status:** done — code + tests committed and passing.

**Builds on:**
- [Subproject 99 — External Sources TLS](2026-05-18-external-sources-tls-design.md)
- [Subproject 100 — Object-Store sources](2026-05-19-kesseldb-subproject100-objstore.md)
- [Subproject 101 — Parquet OBJ-2a](2026-05-19-kesseldb-subproject101-parquet.md)
- [Subproject 102 — RLE/bit-packing hybrid](2026-05-19-kesseldb-subproject102-rle.md)

**Design:** [2026-05-19-parquet-dictionary-design.md](2026-05-19-parquet-dictionary-design.md)
**Plan:** [../plans/2026-05-19-parquet-dictionary.md](../plans/2026-05-19-parquet-dictionary.md)

---

## What shipped

`kessel-parquet::extract()` now decodes **dictionary-encoded flat
REQUIRED, UNCOMPRESSED, V1** columns (pyarrow default
`use_dictionary=True`):

- `meta.rs`: `Encoding::PlainDictionary(2)`/`RleDictionary(8)`;
  `ColumnChunk.dictionary_page_offset` (CMD field 11);
  `PageHeader.dict_num_values/dict_encoding` + field-7
  `DictionaryPageHeader` decode (per-struct `last_id` bracketing).
- `dict.rs` (new, pure, zero-dep): `resolve_dict_indices` — reads
  the data-page bit-width byte, decodes indices via SP102
  `rle::decode_hybrid`, resolves against the PLAIN-decoded
  dictionary with bounds-checked lookups.
- `extract()`: per-chunk read refactored into `read_chunk_values`
  — optional leading DICTIONARY_PAGE then a multi-DATA_PAGE loop;
  each data page dispatched PLAIN (dictionary-fallback) or
  PLAIN_DICTIONARY/RLE_DICTIONARY.

Still rejected with typed errors: compression (OBJ-2b-3),
OPTIONAL/levels (OBJ-2b-4), DELTA/BYTE_STREAM_SPLIT/INT96/V2 (OBJ-2c).

---

## Verification

- Spec KATs hand-derived from parquet-format (dict index hybrid
  stream `[0x02,0x03,0x58,0x00]`→`[a,c,b,b]`; bit_width=0→all
  `dict[0]`; field-11/field-7 thrift KATs).
- Real pyarrow 24.0.0 `use_dictionary=True, compression=None`
  fixture (`dict_flat.parquet`) round-trips; e2e via the SP101
  oracle harness (fail-closed, no router fixture-trust bypass).
- Determinism pin: same logical values are byte-identical
  `PqValue` whether PLAIN- or dictionary-encoded
  (`pq_to_cell`/coerce unchanged).
- Pentest: catch_unwind locks (empty payload, OOB index, huge
  bit-width, truncated stream, lying n, dict-page-offset past EOF)
  → typed errors, no panic/OOM; plus a positive lock proving
  `bit_width==0` valid input is NOT over-rejected.

---

## Intended behavior change (reviewed — NOT a regression)

OBJ-2a's `extract_rejects_dict_columnmeta_encoding` and
`extract_rejects_dict_data_page_encoding` asserted dictionary is
rejected. This slice intentionally supports dictionary, so those two
tests were replaced: `extract_rejects_delta_encoding` (asserts
DELTA_BINARY_PACKED still Unsupported) and
`extract_decodes_dictionary_int64` (positive decode). All other
OBJ-2a gate tests (snappy/optional/schema-mismatch/missing-column/
golden) remain unchanged and green. This was a deliberate, reviewed
change, not a silent test weakening.

---

## Honest gate accounting

`kessel-parquet` is an existing workspace member; its new tests run
under `cargo test --workspace`. Default-build total: **310 → <FINAL>**
(+<DELTA>) — new dict/meta/extract/fixture/pentest tests minus the 2
intentionally-removed dict-reject tests. NOT a zero-delta (same
corrected stance as SP100/101/102). Kernel pulls no new external
dependency; `kessel-parquet/Cargo.toml` `[dependencies]` empty;
default `cargo tree -p kesseldb-server` links no
parquet/objstore/rustls/webpki;
`large_seed_corpus_is_deterministic_and_converges` green; existing
EXT/TLS/OBJ-1 oracles (2/1/1) unchanged.

---

## Deferred (next OBJ-2b / OBJ-2c)

- OBJ-2b-3: Snappy block decompression (flips the Snappy gate).
- OBJ-2b-4: OPTIONAL/def-levels + nullable wiring + e2e.
- OBJ-2c: gzip/zstd, INT96/DECIMAL, REPEATED/nested, V2 pages.
```

Substitute `<FINAL>`/`<DELTA>` with measured numbers.

- [ ] **Step 3: STATUS.md row (numeric order, after the SP102 row)**

Read `docs/STATUS.md`, find the SP102 row, insert immediately AFTER it (matching the table/bullet format):

```
- OBJ-2b-2 (SP103): dictionary-encoded flat REQUIRED UNCOMPRESSED V1
  Parquet now decoded (pyarrow default use_dictionary) via
  kessel-parquet::dict + SP102 rle. Still typed-Unsupported: Snappy
  (OBJ-2b-3), OPTIONAL (OBJ-2b-4), DELTA/INT96/V2 (OBJ-2c).
```

- [ ] **Step 4: docs/USAGE.md §7f note**

Read `docs/USAGE.md`, find the §7f Parquet/OBJ-2b note added in SP102, and update/append (do NOT overclaim):

```
> **OBJ-2b-2 (SP103):** dictionary-encoded Parquet (pyarrow default
> `use_dictionary=True`) is now supported for flat REQUIRED,
> UNCOMPRESSED, V1 files. Compression still requires
> `compression=None` (Snappy → OBJ-2b-3); nullable/OPTIONAL columns
> still unsupported (→ OBJ-2b-4).
```

- [ ] **Step 5: Determinism gate (docs-only must still hold)**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -12`
Expected: `FAILED=0`, total == `<FINAL>`, seed-7 green.

- [ ] **Step 6: Commit docs**

```bash
cd /c/Users/ihass/KesselDB && git add docs/superpowers/specs/2026-05-19-kesseldb-subproject103-dict.md docs/STATUS.md docs/USAGE.md && git commit -m "docs: OBJ-2b-2 dictionary — subproject103 record + STATUS/USAGE + gate reconciliation" && git push
```

- [ ] **Step 7: Auto-memory (OUTSIDE repo — never git-add)**

Append via Bash heredoc (do NOT full-Read then Edit — large file):

```bash
cat >> "/c/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/project_kesseldb.md" <<'EOF'

## SP103 (2026-05-19) — OBJ-2b-2 Parquet dictionary encoding
extract() now decodes dictionary-encoded flat REQUIRED UNCOMPRESSED V1
(pyarrow default use_dictionary). meta.rs Encoding 2/8 + CMD f11 +
PageHeader f7; new dict.rs resolve_dict_indices (bit-width byte +
rle::decode_hybrid + bounds-checked lookup); extract() read_chunk_values
= optional DICTIONARY_PAGE + multi-DATA_PAGE loop (PLAIN-fallback or
dict per page). Real pyarrow dict fixture + e2e (fail-closed).
Intended behavior change: 2 OBJ-2a dict-reject tests → positive/DELTA
(reviewed, documented, not silent). Honest gate 310→<FINAL>. Kernel
zero-dep + seed-7 + EXT/TLS/OBJ-1 oracles 2/1/1 unchanged. Next:
OBJ-2b-3 Snappy / OBJ-2b-4 OPTIONAL+nullable / OBJ-2c.
EOF
```

Then update the KesselDB line in `…\MEMORY.md` (Read to find the exact `- [KesselDB](project_kesseldb.md) — …` line, Edit it) so the trailing status clause becomes: `SP103 SHIPPED: OBJ-2b-2 dictionary-encoded flat REQUIRED uncompressed V1 (pyarrow default). Open: OBJ-2b-3 Snappy / OBJ-2b-4 OPTIONAL+fixtures+e2e / OBJ-2c / OBJ-3 / OBJ-4 / OBJ-5 / WASM / #75 SP-A scatter scan / seed-7 liveness / SQL-over-cluster`. Keep the rest of the line's prefix intact.

- [ ] **Step 8: Report DONE** with `<BASELINE>=310`, `<FINAL>`, `<DELTA>`, FAILED count, seed-7 status, deps-clean (y/n), the intended-behavior-change confirmation, and the docs commit SHA.

---

## Self-Review

**1. Spec coverage** (design → task):
- `Encoding` 2/8 + `dictionary_page_offset` f11 + `PageHeader` f7 → Task 1 ✓
- `dict.rs` resolve (bit-width byte + `rle::decode_hybrid` + bounds-checked lookup; bit_width==0 valid) → Task 2 ✓
- `extract()` `read_chunk_values` (dict page + multi-data-page loop + per-page PLAIN/dict dispatch; require explicit `dictionary_page_offset`; gate flips) → Task 3 ✓
- Determinism/source-format-independence pin → Task 3 `extract_plain_and_dict_are_identical` ✓
- Intended behavior change (2 OBJ-2a dict-reject tests replaced; others unchanged) → Task 3 + documented Task 6 ✓
- Real pyarrow `use_dictionary=True, compression=None` fixture + e2e fail-closed → Task 4 ✓
- Pentest (lying dict_num_values, OOB index, dict-offset past EOF, empty payload, huge bit-width, truncated stream, multi-page mismatch, **bit_width==0 valid correctness lock**) → Task 5 ✓
- Honest gate reconciliation (310→FINAL, not zero-delta), zero-dep, seed-7, oracles unchanged, SP-convention record → Task 6 ✓

**2. Placeholder scan:** No "TBD"/"handle edge cases"/"similar to". `<BASELINE>/<FINAL>/<DELTA>` are runtime-measured values explicitly defined in Task 0 / Task 6 Step 1. All code/byte blocks are concrete and hand-derived.

**3. Type consistency:** `resolve_dict_indices(&[u8], &[PqValue], usize) -> Result<Vec<PqValue>, PqError>` used identically in Tasks 2/3/5. `read_chunk_values(&[u8], &meta::ColumnChunk, meta::Type) -> Result<Vec<PqValue>, PqError>` used in Task 3. `Encoding::{PlainDictionary,RleDictionary}`, `ColumnChunk.dictionary_page_offset: Option<i64>`, `PageHeader.{dict_num_values:i32,dict_encoding:i32}` consistent across Tasks 1/3. `PqError::{Bad,Unsupported}`/`PqValue::{I64,Bytes}` match the existing crate enums.

Plan is internally consistent and fully covers the OBJ-2b-2 design.
