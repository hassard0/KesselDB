# OBJ-2c-3 Parquet V2 Data Pages Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decode Parquet `DATA_PAGE_V2` (pyarrow `data_page_version='2.0'`) for the matrix already supported in V1 (flat REQUIRED|OPTIONAL × UNCOMPRESSED|Snappy|GZIP × PLAIN|dict), reusing every shipped primitive; V1 decode stays byte-identical.

**Architecture:** `meta.rs` learns the field-8 `DataPageHeaderV2` (page_type 3). `lib.rs` adds a dedicated `decode_data_page_v2` (V2 stores levels RAW before the value-section compression, so it bypasses the whole-page `page_payload` seam) and factors the null-scatter into a shared `scatter_nulls` used by both the V1-OPTIONAL arm and V2. Task 1 first extracts the now-6×-duplicated fail-closed e2e into a shared helper (behavior-preserving). No kessel-fetch/kessel-sql/server/kernel production change.

**Tech Stack:** Rust (workspace edition), `#![forbid(unsafe_code)]`, zero external deps, existing `PqError`/`PqValue`, `rle::decode_hybrid`/`plain::decode_plain`/`dict::resolve_dict_indices`/`snappy::decompress`/`gzip::decompress`.

---

## Context for the implementer (read once)

`crates/kessel-parquet` modules: `thrift.rs`, `footer.rs`, `meta.rs`, `plain.rs`, `rle.rs`, `dict.rs`, `snappy.rs`, `gzip.rs`, `lib.rs`. `Cargo.toml` `[dependencies]` empty and MUST stay empty.

- `meta.rs` `decode_page_header` decodes thrift PageHeader `{1:type, 2:uncompressed_page_size, 3:compressed_page_size, 5:DataPageHeader{1:num_values,2:encoding}, 7:DictionaryPageHeader{1:num_values,2:encoding,3:is_sorted}}`; field 4 (crc) skipped. `PageHeader` struct holds `page_type, uncompressed_size, compressed_size, dp_num_values, dp_encoding, dict_num_values, dict_encoding`. The field-5/7 arms use the SP101 per-struct `s.save_last_id()/reset_last_id()/restore_last_id()` bracketing.
- `lib.rs:51 fn page_payload(...)` — whole-page slice+decompress seam (Uncompressed→Borrowed, Snappy/Gzip→Owned, Other→Unsupported).
- `lib.rs:87 fn decode_page(payload,dp_encoding,wp,n,max_def_level,dict)` — `max_def_level==0`: PLAIN/dict over the whole payload for `n`; `max_def_level==1`: `rle::decode_level_v1(payload,1,n)` (4-byte-u32-LE-len-prefixed) → present → values over `payload[consumed..]` → null-scatter.
- `lib.rs:~224` data-page loop: `if ph.page_type != 0 { return Err(PqError::Unsupported("non-V1 data page (V2/index): OBJ-2c".into())); }` then `n = ph.dp_num_values`, `payload = page_payload(...)`, `vals = decode_page(&payload, ph.dp_encoding, want_ptype, n, max_def_level, &dict)`.
- `lib.rs:187` dict page requires `ph.page_type == 2`.
- `crates/kesseldb-server/tests/external_source_parquet_oracle.rs` has 5 near-identical `refresh_*_parquet_from_s3_fails_closed_and_state_intact` fns (SP101 flat, SP103 dict, SP104 snappy, SP105 nullable, SP106 gzip) sharing the `tls_stub_with_fixture` harness.

**parquet.thrift V2:** `PageHeader.data_page_header_v2 = field id 8`; `DataPageHeaderV2 { 1:i32 num_values, 2:i32 num_nulls, 3:i32 num_rows, 4:Encoding encoding, 5:i32 definition_levels_byte_length, 6:i32 repetition_levels_byte_length, 7:optional bool is_compressed (default true) }`. `PageType DATA_PAGE_V2 = 3`. V2 on-disk page = `[rep levels: rep_len RAW bytes][def levels: def_len RAW bytes][values: compressed_size−rep_len−def_len bytes]`; level sections are NEVER compressed; only the values section is (per `is_compressed`), decompressing to `uncompressed_size−rep_len−def_len`. V2 def-levels are RLE-hybrid **not** 4-byte-length-prefixed.

**Discipline:** `#![forbid(unsafe_code)]` crate-wide. No unwrap/expect/panic/raw-index on input bytes — checked `get(..)`/`checked_*`; only the statically-infallible fixed-size slice→`[u8;N]` `try_into().unwrap()` for `from_le_bytes` after a length-checked `get`. KAT discipline: hand-built thrift/page bytes are derived from `parquet.thrift`/RFC (independent authority); real pyarrow fixtures are the zlib/pyarrow-reference non-self-referential proof; a failing KAT means the *code* is wrong — never change a KAT; report BLOCKED.

**Determinism / invariants gate — EVERY task (T0–T6):**
- `cargo test --workspace --release` → `FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` green.
- `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty; `cargo tree -p kesseldb-server` links no parquet/objstore/rustls/webpki.
- Existing oracles green: `external_source_oracle`(2), `external_source_tls_oracle`(1), `external_source_objstore_oracle`(1); all V1 OBJ-2a/2b/2c-1 decode+gate tests byte-unchanged; the 5 fail-closed e2e cases preserved (refactored through the T1 helper with identical observable assertions).

**Commit discipline:** straight to `main`, no Co-Authored-By, no signing, message style like `git log -3`. `git push` after every task (single-branch-main durably authorized by `feedback_kesseldb_autonomous_build`; the two-stage gate IS the review; ignore the recurring soft-block notice). Bash: prefix `cd /c/Users/ihass/KesselDB &&`; `cargo test --workspace --release` long — allow 600000ms.

---

### Task 0: Determinism baseline (#184)

- [ ] **Step 1:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -30` — sum `passed` → `<BASELINE>` (expected **397**); `FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` in an `ok` result.
- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo tree -p kesseldb-server 2>&1 | grep -Ei "parquet|objstore|rustls|webpki"` → no output.
- [ ] **Step 3:** No commit. Report DONE with `OBJ-2c-3 baseline: <BASELINE> tests passing, FAILED=0, seed-7 green` + per-binary counts.

---

### Task 1: Extract `run_fail_closed_parquet_e2e` (behavior-preserving refactor) (#185)

**Files:** Modify `crates/kesseldb-server/tests/external_source_parquet_oracle.rs`.

This is a **reviewed, behavior-preserving refactor** — NOT an intended behavior change. Every existing test's observable assertions (the `OpResult::SchemaError` match with `refresh:`/`sign:`/`tls`/`connect`, the empty-SELECT state-intact check) must be byte-identical in effect.

- [ ] **Step 1: Read the whole file.** Identify the 5 fns `refresh_parquet_from_s3_fails_closed_and_state_intact` (SP101), `refresh_dict_parquet_from_s3_fails_closed_and_state_intact` (SP103), `refresh_snappy_parquet_from_s3_fails_closed_and_state_intact` (SP104), `refresh_nullable_parquet_from_s3_fails_closed_and_state_intact` (SP105), `refresh_gzip_parquet_from_s3_fails_closed_and_state_intact` (SP106), and the per-test variables that differ: the fixture static, the env-var pair (`OBJ_*_KEYID`/`OBJ_*_SECRET`), the shard tag, the source name. Everything else (the `tls_stub_with_fixture` call, `spawn_shard`, Router/listener, sleep, connect, DDL, the SchemaError match arms, the empty-SELECT assertion) is identical across all 5.

- [ ] **Step 2: Write the shared helper.** Add:
```rust
/// Shared fail-closed e2e: serves `fixture` over a self-signed
/// localhost TLS stub; the production webpki-roots client must
/// reject it; REFRESH must return OpResult::SchemaError (refresh/
/// sign/tls/connect) and the subsequent SELECT must be empty
/// (atomic-abort, state intact). No router fixture-trust bypass.
fn run_fail_closed_parquet_e2e(
    fixture: &'static [u8],
    tag: &str,
    keyid_env: &str,
    secret_env: &str,
    source: &str,
) {
    // ... the EXACT body shared by the 5 existing tests, with the
    // 4 varying values threaded as params. Copy the body verbatim
    // from one existing test (e.g. the SP106 gzip one), replacing
    // its hardcoded fixture/env/tag/source with the params. Keep
    // every assertion, sleep duration, DDL string shape, and the
    // SchemaError match arms identical.
}
```
- [ ] **Step 3: Rewrite the 5 call-sites.** Each existing `#[test] fn refresh_*` becomes a one-liner: `run_fail_closed_parquet_e2e(<FIXTURE_STATIC>, "<tag>", "OBJ_*_KEYID", "OBJ_*_SECRET", "<source>");` preserving each test's original tag/env/source values exactly (so no cross-test collision and identical observable behavior). Keep the `#[test]` attribute, fn names, and doc-comments. Do NOT change the fixture statics or any value — only move the shared body into the helper.

- [ ] **Step 4:** `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources-objstore --test external_source_parquet_oracle 2>&1 | tail -8` → all 5 still pass (0 failed), same names. The observable behavior is identical (same fixtures, same fail-closed assertions, same isolation).

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, total UNCHANGED from baseline (refactor — net-0 test count), seed-7 green.

- [ ] **Step 6: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kesseldb-server/tests/external_source_parquet_oracle.rs && git commit -m "test: extract shared run_fail_closed_parquet_e2e (behavior-preserving; SP106-tracked refactor)" && git push
```

---

### Task 2: `meta.rs` field-8 `DataPageHeaderV2` decode (#186)

**Files:** Modify `crates/kessel-parquet/src/meta.rs`.

- [ ] **Step 1: Write the failing test** — add inside `meta.rs` `#[cfg(test)] mod tests` (has `uv`/`zz`). It hand-builds a PageHeader with `type=DATA_PAGE_V2(3)` + field-8 `DataPageHeaderV2`:

```rust
#[test]
fn pageheader_decodes_data_page_header_v2_field8() {
    // PageHeader { 1:type=DATA_PAGE_V2(3), 2:uncompressed=18,
    //   3:compressed=18, 8:DataPageHeaderV2{1:num_values=3,
    //   2:num_nulls=1, 3:num_rows=3, 4:encoding=PLAIN(0),
    //   5:def_levels_byte_length=2, 6:rep_levels_byte_length=0,
    //   7:is_compressed=false } }
    // compact field header = (delta<<4)|ctype; i32-zigzag=5,
    // struct=12, BOOL_FALSE=2. zz(n)=(n<<1)^(n>>63).
    let mut h = Vec::new();
    h.push(0x15); uv(&mut h, zz(3));   // f1 type=DATA_PAGE_V2(3) (delta 0->1=1,i32)
    h.push(0x15); uv(&mut h, zz(18));  // f2 uncompressed=18 (delta 1->2=1,i32)
    h.push(0x15); uv(&mut h, zz(18));  // f3 compressed=18 (delta 2->3=1,i32)
    h.push(0x5c);                      // f8 DataPageHeaderV2 struct (delta 3->8=5 -> (5<<4)|12=0x5c)
    h.push(0x15); uv(&mut h, zz(3));   // g1 num_values=3 (reset; delta 0->1=1,i32)
    h.push(0x15); uv(&mut h, zz(1));   // g2 num_nulls=1 (delta 1->2=1,i32)
    h.push(0x15); uv(&mut h, zz(3));   // g3 num_rows=3 (delta 2->3=1,i32)
    h.push(0x15); uv(&mut h, zz(0));   // g4 encoding=PLAIN(0) (delta 3->4=1,i32)
    h.push(0x15); uv(&mut h, zz(2));   // g5 def_levels_byte_length=2 (delta 4->5=1,i32)
    h.push(0x15); uv(&mut h, zz(0));   // g6 rep_levels_byte_length=0 (delta 5->6=1,i32)
    h.push(0x12);                      // g7 is_compressed=false (delta 6->7=1, BOOL_FALSE=2 -> 0x12)
    h.push(0x00);                      // stop DataPageHeaderV2
    h.push(0x00);                      // stop PageHeader

    let (ph, _len) = decode_page_header(&h).expect("decode");
    assert_eq!(ph.page_type, 3);
    assert_eq!(ph.v2_num_values, 3);
    assert_eq!(ph.v2_num_nulls, 1);
    assert_eq!(ph.v2_encoding, 0);
    assert_eq!(ph.v2_def_len, 2);
    assert_eq!(ph.v2_rep_len, 0);
    assert_eq!(ph.v2_is_compressed, false);
}
```

- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet pageheader_decodes_data_page_header_v2_field8 2>&1 | tail -8` → compile error (no `v2_*` fields).

- [ ] **Step 3: Implement.** Extend `PageHeader`:
```rust
// V2 (DataPageHeaderV2, PageHeader field 8). Only meaningful when
// page_type == 3 (DATA_PAGE_V2). v2_is_compressed defaults true
// (the thrift field is optional, default true).
pub v2_num_values: i32,
pub v2_num_nulls: i32,
pub v2_num_rows: i32,
pub v2_encoding: i32,        // default -1
pub v2_def_len: i32,
pub v2_rep_len: i32,
pub v2_is_compressed: bool,  // default true
```
In `decode_page_header`, initialise `v2_num_values:0, v2_num_nulls:0, v2_num_rows:0, v2_encoding:-1, v2_def_len:0, v2_rep_len:0, v2_is_compressed:true`. Add the field-8 arm immediately after the field-7 arm, mirroring field-7's structure exactly (the `if f.id == 7` style + `s.save_last_id(); s.reset_last_id(); ... s.restore_last_id(saved);` bracketing — copy the exact bracketing the existing field-5/7 arms use):
```rust
8 => {
    // DataPageHeaderV2 nested struct (per-struct last_id reset,
    // same bracketing as field 5 / field 7).
    let saved = s.save_last_id();
    s.reset_last_id();
    loop {
        let f2 = s.read_field_header()?;       // use the same nested-field read the f5/f7 arms use
        if f2.is_stop() { break; }
        match f2.id {
            1 => ph.v2_num_values = s.read_i32(&f2)?,
            2 => ph.v2_num_nulls = s.read_i32(&f2)?,
            3 => ph.v2_num_rows = s.read_i32(&f2)?,
            4 => ph.v2_encoding = s.read_i32(&f2)?,
            5 => ph.v2_def_len = s.read_i32(&f2)?,
            6 => ph.v2_rep_len = s.read_i32(&f2)?,
            7 => ph.v2_is_compressed = s.read_bool(&f2)?,
            _ => s.skip_field(&f2)?,
        }
    }
    s.restore_last_id(saved);
}
```
**IMPORTANT:** the exact nested-field-read / stop / skip / bool API names above are illustrative — READ how the existing field-5 and field-7 arms iterate the nested struct (the precise `StructReader` method names, the stop sentinel, how bool is read — thrift-compact encodes bool in the field-type nibble so `read_bool` may be `f2.bool_val()` or similar) and MIRROR THAT EXACTLY. The field-8 arm must use the identical nested-iteration mechanism + the identical per-struct `last_id` save/reset/restore the field-7 arm uses (the SP101 fix). If `is_compressed` (a thrift bool) needs special handling vs i32 fields, match how any existing bool (e.g. DictionaryPageHeader field-3 `is_sorted`) is read/skipped. Keep field 1/2/3/5/7 decode byte-identical (V1 unaffected).

- [ ] **Step 4:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet meta:: 2>&1 | tail -12` → new test + ALL pre-existing meta tests pass (V1 PageHeader / dict / codec tests unchanged: they never set field 8, and `v2_*` default to the initialised values).

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, total ≥ baseline+1, seed-7 green.

- [ ] **Step 6: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/meta.rs && git commit -m "parquet: meta DataPageHeaderV2 decode (PageHeader field 8)" && git push
```

---

### Task 3: `decode_data_page_v2` + shared `scatter_nulls` + page_type==3 gate flip (#187)

**Files:** Modify `crates/kessel-parquet/src/lib.rs`.

Before coding: READ `decode_page` (`lib.rs:87`), the data-page loop gate (`lib.rs:~224`), and the SP104/SP105 hand-builders (`build_snappy_plain_int64_file`, `build_opt_plain_i64`/`build_opt_plain_i64_inner`) — the V2 builders mirror them with the field-8 header.

- [ ] **Step 1: Add the V2 hand-builders + tests** inside `lib.rs` `mod tests`:

```rust
/// V2 PLAIN INT64 file. `defs` (rows, 0/1) + `present_vals` (the
/// non-null i64s). REQUIRED ⇒ defs all 1 & def_len 0; OPTIONAL ⇒
/// def-level RLE-hybrid bytes (NOT 4-byte-prefixed) of length
/// def_len. codec=UNCOMPRESSED, is_compressed=false. Layout:
/// [PAR1][PageHeaderV2][rep(0)][def(def_len)][values PLAIN]
/// [FileMetaData][mlen u32 LE][PAR1].
fn build_v2_plain_i64(defs: &[u8], present_vals: &[i64], optional: bool) -> Vec<u8> {
    assert_eq!(present_vals.len(), defs.iter().filter(|&&d| d==1).count());
    let n = defs.len();
    let nulls = defs.iter().filter(|&&d| d==0).count();
    // def-level section: OPTIONAL ⇒ one bit-packed group of 8,
    // bit_width 1, NOT length-prefixed (V2). header=(1<<1)|1=0x03;
    // bits byte: bit i set iff defs[i]==1 (i<8). REQUIRED ⇒ empty.
    let def_section: Vec<u8> = if optional {
        let mut bits = 0u8;
        for (i,&d) in defs.iter().enumerate() { if d==1 && i<8 { bits |= 1<<i; } }
        vec![0x03, bits]
    } else { Vec::new() };
    let def_len = def_section.len() as i64;
    let mut values = Vec::new();
    for v in present_vals { values.extend_from_slice(&v.to_le_bytes()); }
    // page payload = [rep(0)][def_section][values]
    let mut payload = Vec::new();
    payload.extend_from_slice(&def_section);
    payload.extend_from_slice(&values);
    let psz = payload.len() as i64;        // uncompressed == compressed (Uncompressed)

    // PageHeader: f1 type=DATA_PAGE_V2(3), f2 uncompressed=psz,
    // f3 compressed=psz, f8 DataPageHeaderV2{1:num_values=n,
    // 2:num_nulls=nulls, 3:num_rows=n, 4:encoding=PLAIN(0),
    // 5:def_levels_byte_length=def_len, 6:rep_levels_byte_length=0,
    // 7:is_compressed=false}.
    let mut hdr = Vec::new();
    hdr.push(0x15); uv(&mut hdr, zz(3));            // f1 type=3
    hdr.push(0x15); uv(&mut hdr, zz(psz));          // f2 uncompressed
    hdr.push(0x15); uv(&mut hdr, zz(psz));          // f3 compressed
    hdr.push(0x5c);                                 // f8 struct (delta 3->8=5)
    hdr.push(0x15); uv(&mut hdr, zz(n as i64));     // g1 num_values
    hdr.push(0x15); uv(&mut hdr, zz(nulls as i64)); // g2 num_nulls
    hdr.push(0x15); uv(&mut hdr, zz(n as i64));     // g3 num_rows
    hdr.push(0x15); uv(&mut hdr, zz(0));            // g4 encoding=PLAIN
    hdr.push(0x15); uv(&mut hdr, zz(def_len));      // g5 def_levels_byte_length
    hdr.push(0x15); uv(&mut hdr, zz(0));            // g6 rep_levels_byte_length
    hdr.push(0x12);                                 // g7 is_compressed=false
    hdr.push(0x00); hdr.push(0x00);                 // stop DPHv2 / stop PageHeader

    let data_page_offset: i64 = 4;
    // FileMetaData: 1 INT64 "id" leaf, repetition REQUIRED(0) or
    // OPTIONAL(1), 1 RG, 1 ColumnChunk, encodings[PLAIN], codec
    // UNCOMPRESSED(0), num_values=n, data_page_offset. (Mirror the
    // SP104/SP105 build_*_file FileMetaData byte sequence exactly,
    // swapping repetition zz(if optional {1} else {0}) and
    // num_values/num_rows = n.)
    let mut m = Vec::new();
    m.push(0x15); uv(&mut m, zz(2));                // f1 version=2
    m.push(0x19); m.push(0x2c);                     // f2 list<SchemaElement> 2
    m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
    m.push(0x15); uv(&mut m, zz(1));                // root num_children=1
    m.push(0x00);
    m.push(0x15); uv(&mut m, zz(2));                // leaf f1 type=INT64
    m.push(0x25); uv(&mut m, zz(if optional {1} else {0})); // f3 repetition
    m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
    m.push(0x00);
    m.push(0x16); uv(&mut m, zz(n as i64));         // f3 num_rows
    m.push(0x19); m.push(0x1c);                     // f4 list<RowGroup> 1
    m.push(0x19); m.push(0x1c);                     // RG f1 list<ColumnChunk> 1
    m.push(0x3c);                                   // ColumnChunk f3 CMD
    m.push(0x15); uv(&mut m, zz(2));                // CMD f1 type=INT64
    m.push(0x19); m.push(0x15); uv(&mut m, zz(0));  // f2 enc [PLAIN]
    m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
    m.push(0x15); uv(&mut m, zz(0));                // f4 codec=UNCOMPRESSED
    m.push(0x16); uv(&mut m, zz(n as i64));         // f5 num_values
    m.push(0x46); uv(&mut m, zz(data_page_offset)); // f9 data_page_offset
    m.push(0x00); m.push(0x00);                     // stop CMD / ColumnChunk
    m.push(0x26); uv(&mut m, zz(n as i64));         // RG f3 num_rows
    m.push(0x00); m.push(0x00);                     // stop RG / FileMetaData

    let mut f = Vec::new();
    f.extend_from_slice(b"PAR1");
    f.extend_from_slice(&hdr);
    f.extend_from_slice(&payload);
    let mlen = m.len() as u32;
    f.extend_from_slice(&m);
    f.extend_from_slice(&mlen.to_le_bytes());
    f.extend_from_slice(b"PAR1");
    f
}

#[test]
fn extract_decodes_v2_plain_required_int64() {
    let f = build_v2_plain_i64(&[1,1], &[7,-2], false);
    assert_eq!(extract(&f, &["id"]).expect("v2 req"),
        vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]);
}

#[test]
fn extract_decodes_v2_plain_optional_int64_with_nulls() {
    // [7, null, -2]: defs [1,0,1], present [7,-2]; def_section
    // = [0x03, 0b00000101=0x05] (NOT 4-byte-prefixed — V2).
    let f = build_v2_plain_i64(&[1,0,1], &[7,-2], true);
    assert_eq!(extract(&f, &["id"]).expect("v2 opt"),
        vec![vec![PqValue::I64(7)], vec![PqValue::Null], vec![PqValue::I64(-2)]]);
}

#[test]
fn extract_v2_and_v1_plain_identical() {
    // Source-format independence: same logical [7,-2] V2 vs the
    // existing V1 build_parquet_file(0,0,0,false).
    let v2 = extract(&build_v2_plain_i64(&[1,1], &[7,-2], false), &["id"]).unwrap();
    let v1 = extract(&build_parquet_file(0,0,0,false), &["id"]).unwrap();
    assert_eq!(v2, v1);
    assert_eq!(v2, vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]);
}
```
Also add a **V2 + dictionary** test `extract_decodes_v2_dict_int64`: a file with a V1-style DICTIONARY_PAGE (reuse the SP103 dict-page builder bytes) at `dictionary_page_offset` then a DATA_PAGE_V2 whose values section (after the rep(0)/def split) is `[bit_width byte][RLE-hybrid dict indices]`; logical `[7,null,7]` dict `[7]` → `[[I64(7)],[Null],[I64(7)]]`. Spell the full builder by composing the SP103 dict-page bytes + the V2 header above (def_section `[0x03,0x05]` for `[1,0,1]`, values section `[0x01,0x03,0x00]` per the SP105 opt-dict KAT). (The heavy dict/compression composition is also proven non-self-referentially by the real-pyarrow T4 fixtures; this hand KAT pins the V2 level-split + dict path.)

- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet extract_decodes_v2_plain_required_int64 2>&1 | tail -8` → FAIL (page_type==3 rejected).

- [ ] **Step 3: Implement.**
  1. **Factor the shared scatter.** Locate the V1 `decode_page` `max_def_level==1` arm's null-scatter (build `Vec<PqValue>` of length `n`, walking `defs`: `d==1` → next decoded value via an iterator with a count-mismatch `Bad`, `d==0` → `PqValue::Null`). Extract it verbatim into:
     ```rust
     fn scatter_nulls(defs: &[u64], vals: Vec<PqValue>, n: usize)
         -> Result<Vec<PqValue>, PqError> { /* exact moved logic */ }
     ```
     Replace the V1 arm's inline scatter with `scatter_nulls(&defs, vals, n)?`. The V1 REQUIRED/OPTIONAL observable behavior MUST be byte-identical (same logic, relocated; verify by the unchanged V1 tests passing).
  2. **`decode_data_page_v2`:**
     ```rust
     fn decode_data_page_v2(
         region: &[u8],            // raw file[dstart..dstart+compressed_size]
         ph: &meta::PageHeader,
         codec: meta::Codec,
         want_ptype: meta::Type,
         max_def_level: u32,
         dict: &[PqValue],
     ) -> Result<Vec<PqValue>, PqError> {
         let rep_len = usize::try_from(ph.v2_rep_len)
             .map_err(|_| PqError::Bad("v2 rep_len range".into()))?;
         if rep_len > 0 {
             return Err(PqError::Unsupported(
                 "REPEATED/nested V2 (repetition levels): OBJ-2c-5".into()));
         }
         let def_len = usize::try_from(ph.v2_def_len)
             .map_err(|_| PqError::Bad("v2 def_len range".into()))?;
         let n = usize::try_from(ph.v2_num_values)
             .map_err(|_| PqError::Bad("v2 num_values range".into()))?;
         let lvl_end = rep_len.checked_add(def_len)
             .ok_or_else(|| PqError::Bad("v2 level len ovf".into()))?;
         if lvl_end > region.len() {
             return Err(PqError::Bad("v2 levels exceed page".into()));
         }
         let def_bytes = region
             .get(rep_len..lvl_end)
             .ok_or_else(|| PqError::Bad("v2 def slice".into()))?;
         let values_section = region
             .get(lvl_end..)
             .ok_or_else(|| PqError::Bad("v2 values slice".into()))?;
         // def-levels
         let (defs, present): (Option<Vec<u64>>, usize) =
             if max_def_level == 1 {
                 let d = rle::decode_hybrid(def_bytes, 1, n)?;
                 if d.len() != n { return Err(PqError::Bad("v2 def count".into())); }
                 if d.iter().any(|&x| x > 1) {
                     return Err(PqError::Bad("v2 def-level exceeds max".into()));
                 }
                 let p = d.iter().filter(|&&x| x==1).count();
                 // defense-in-depth: cross-check vs declared num_nulls
                 let nn = usize::try_from(ph.v2_num_nulls)
                     .map_err(|_| PqError::Bad("v2 num_nulls range".into()))?;
                 if n.checked_sub(nn) != Some(p) {
                     return Err(PqError::Bad(
                         "v2 num_nulls vs def-levels mismatch".into()));
                 }
                 (Some(d), p)
             } else {
                 if def_len != 0 {
                     return Err(PqError::Bad("v2 def_len non-zero for REQUIRED".into()));
                 }
                 (None, n)
             };
         // values: target uncompressed length
         let uncomp = usize::try_from(ph.uncompressed_size)
             .map_err(|_| PqError::Bad("v2 uncompressed size range".into()))?;
         let vt = uncomp.checked_sub(lvl_end)
             .ok_or_else(|| PqError::Bad("v2 values target underflow".into()))?;
         let values_raw: std::borrow::Cow<[u8]> = match codec {
             meta::Codec::Uncompressed => std::borrow::Cow::Borrowed(values_section),
             _ if !ph.v2_is_compressed => std::borrow::Cow::Borrowed(values_section),
             meta::Codec::Snappy =>
                 std::borrow::Cow::Owned(snappy::decompress(values_section, vt)?),
             meta::Codec::Gzip =>
                 std::borrow::Cow::Owned(gzip::decompress(values_section, vt)?),
             meta::Codec::Other(_) => return Err(PqError::Unsupported(
                 "compression codec (zstd/lz4/brotli): OBJ-2c".into())),
         };
         // when uncompressed/raw, values_section must be exactly vt
         if matches!(codec, meta::Codec::Uncompressed) || !ph.v2_is_compressed {
             if values_section.len() != vt {
                 return Err(PqError::Bad("v2 raw values length mismatch".into()));
             }
         }
         let vals = match ph.v2_encoding {
             0 => plain::decode_plain(&values_raw, want_ptype, present)?,
             2 | 8 => dict::resolve_dict_indices(&values_raw, dict, present)?,
             _ => return Err(PqError::Unsupported(
                 "data page encoding (DELTA/BYTE_STREAM_SPLIT): OBJ-2c".into())),
         };
         match defs {
             Some(d) => scatter_nulls(&d, vals, n),
             None => { if vals.len()!=n {
                 return Err(PqError::Bad("v2 value count".into())); } Ok(vals) }
         }
     }
     ```
  3. **Gate flip** in the data-page loop. Replace `if ph.page_type != 0 { Unsupported("non-V1 data page (V2/index): OBJ-2c") }` + the V1 `payload`/`decode_page` block with:
     ```rust
     let vals = match ph.page_type {
         0 => {
             // existing V1 path — byte-identical (page_payload + decode_page)
             let payload = page_payload(bytes, dstart, comp, uncomp, cc.codec)?;
             let n = usize::try_from(ph.dp_num_values)
                 .map_err(|_| PqError::Bad("num_values range".into()))?;
             decode_page(&payload, ph.dp_encoding, want_ptype, n, max_def_level, &dict)?
         }
         3 => {
             let region = bytes.get(dstart..dstart
                 .checked_add(comp).ok_or_else(|| PqError::Bad("v2 region ovf".into()))?)
                 .ok_or_else(|| PqError::Bad("v2 page truncated".into()))?;
             decode_data_page_v2(region, &ph, cc.codec, want_ptype, max_def_level, &dict)?
         }
         _ => return Err(PqError::Unsupported(
             "non-V1/V2 data page (index): OBJ-2c".into())),
     };
     ```
     (Adapt to the exact existing variable names — `bytes`/`dstart`/`comp`/`uncomp` are whatever the current loop uses; preserve the V1 branch's existing computations EXACTLY so V1 is byte-identical; only restructure the gate into the `match ph.page_type`. The multi-page accumulation (`vals.len()` rows summing to `cc.num_values`) and the dict-page handling at `lib.rs:187` stay unchanged — V2 `num_values` is the row count incl nulls just like V1 `dp_num_values`.)

- [ ] **Step 4:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet 2>&1 | tail -25` → the 4 new V2 tests pass; EVERY existing V1 OBJ-2a/2b/2c-1 test (golden/dict/snappy/gzip/optional/nullable/delta/schema-mismatch/missing-column/truncated/nested/zstd-reject + the lying-comp-size locks) passes byte-unchanged; `FAILED=0`.

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green. Record measured total.

- [ ] **Step 6: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/lib.rs && git commit -m "parquet: decode DATA_PAGE_V2 (decode_data_page_v2 + shared scatter_nulls; V1 byte-identical)" && git push
```

---

### Task 4: Real pyarrow V2 fixtures + roundtrips + e2e (#188)

**Files:** Create `crates/kessel-parquet/tests/fixtures/{v2_plain,v2_dict,v2_nullable,v2_gzip}.parquet`; modify `.../README.md`, `crates/kessel-parquet/tests/fixture_roundtrip.rs`, `crates/kesseldb-server/tests/external_source_parquet_oracle.rs`.

- [ ] **Step 1: Generate (real pyarrow 24.0.0, `data_page_version='2.0'`):**
```bash
cd /c/Users/ihass/KesselDB && python -c "
import pyarrow as pa, pyarrow.parquet as pq
schR = pa.schema([pa.field('id', pa.int64(), nullable=False),
                  pa.field('s',  pa.large_utf8(), nullable=False)])
tR = pa.table({'id': pa.array([7,7,-2,7,100], type=pa.int64()),
               's':  pa.array(['a','a','b','c','a'], type=pa.large_utf8())}, schema=schR)
pq.write_table(tR,'crates/kessel-parquet/tests/fixtures/v2_plain.parquet',
               use_dictionary=False, compression=None, version='1.0', data_page_version='2.0')
pq.write_table(tR,'crates/kessel-parquet/tests/fixtures/v2_dict.parquet',
               use_dictionary=True, compression=None, version='1.0', data_page_version='2.0')
pq.write_table(tR,'crates/kessel-parquet/tests/fixtures/v2_gzip.parquet',
               use_dictionary=True, compression='gzip', version='1.0', data_page_version='2.0')
tN = pa.table({'id': pa.array([7,7,None,-2,100], type=pa.int64()),
               's':  pa.array(['a',None,'b','c','a'], type=pa.large_utf8())})
pq.write_table(tN,'crates/kessel-parquet/tests/fixtures/v2_nullable.parquet',
               version='1.0', data_page_version='2.0')
print('wrote v2_plain + v2_dict + v2_gzip + v2_nullable rows=5')
"
```
If pyarrow fails → STOP, BLOCKED.

- [ ] **Step 2: Metadata-verify the data pages are genuinely DataPageHeaderV2** (not silently V1) for all four. pyarrow's high-level metadata doesn't expose page-header type directly; use the low-level API:
```bash
cd /c/Users/ihass/KesselDB && python -c "
import pyarrow.parquet as pq
for f in ['v2_plain','v2_dict','v2_gzip','v2_nullable']:
  pf=pq.ParquetFile(f'crates/kessel-parquet/tests/fixtures/{f}.parquet')
  rg=pf.metadata.row_group(0)
  print(f, pf.schema_arrow, [(rg.column(i).compression, rg.column(i).encodings) for i in range(pf.metadata.num_columns)])
  t=pq.read_table(f'crates/kessel-parquet/tests/fixtures/{f}.parquet'); print(' ', t.column('id').to_pylist(), t.column('s').to_pylist())
"
cd /c/Users/ihass/KesselDB && python -c "
# Confirm DATA_PAGE_V2 by scanning the raw bytes for the thrift
# PageHeader type field. Simpler robust check: pyarrow writes V2
# iff data_page_version='2.0' was honored — assert by re-reading
# AND by the presence of DataPageV2 via the C++ metadata if
# available; if not exposed, the Rust extract() roundtrip in T4
# Step 4 IS the V2-decode proof (our decoder ONLY accepts page_type
# 3 for these via the new path; a silently-V1 file would still
# decode through the V1 path — so additionally assert at least one
# fixture FAILS to decode if the V2 path is disabled is impractical;
# instead: print the first data page header type byte region).
import struct
for f in ['v2_plain','v2_nullable']:
  b=open(f'crates/kessel-parquet/tests/fixtures/{f}.parquet','rb').read()
  # PAR1 ... ; first PageHeader starts at offset 4. Thrift compact:
  # field1 type i32: header byte 0x15 then zigzag varint. V1 DATA_PAGE
  # zz=0 (0x00); DICTIONARY_PAGE zz=4 (0x04); DATA_PAGE_V2 zz=6 (0x06).
  print(f, 'first page f1 bytes:', b[4], b[5])
"
```
Expected: for v2_plain the first data page's `f1` shows DATA_PAGE_V2 (`0x15 0x06`) — i.e. pyarrow honored `data_page_version='2.0'`. (If a leading DICTIONARY_PAGE `0x15 0x04` appears first for v2_dict, that's expected — the dict page is V1-style; the *data* page after it is V2. For v2_plain/v2_nullable with no dict page the first page IS the V2 data page.) If the first non-dict data page is NOT `0x15 0x06` (DATA_PAGE_V2), pyarrow did not write V2 → STOP, BLOCKED (report; do not proceed with a mislabeled fixture).

- [ ] **Step 3: README** — append a `## v2_plain / v2_dict / v2_gzip / v2_nullable .parquet (OBJ-2c-3)` block: the exact regen command + the metadata-verify note ("data pages are DataPageHeaderV2; first-page f1 = 0x15 0x06 for the no-dict files") + expected rows (incl. v2_nullable nulls). Mirror the SP106 README entry style.

- [ ] **Step 4: Roundtrip** — READ `fixture_roundtrip.rs` for the convention; add a test loading all four via `kessel_parquet::extract(&bytes,&["id","s"])` asserting: v2_plain/v2_dict/v2_gzip → `[[I64(7),Bytes("a")],[I64(7),Bytes("a")],[I64(-2),Bytes("b")],[I64(7),Bytes("c")],[I64(100),Bytes("a")]]`; v2_nullable → `[[I64(7),Bytes("a")],[I64(7),Null],[Null,Bytes("b")],[I64(-2),Bytes("c")],[I64(100),Bytes("a")]]`. (Roundtrip through production `extract()` over metadata-verified-V2 real pyarrow files is the decisive non-self-referential proof; v2_gzip proves V2 per-section gzip decompression composes; v2_nullable proves V2 def-level scatter.)

- [ ] **Step 5: V2-vs-V1 source-independence pin** — add a test: write (or reuse a checked-in V1 dict fixture from SP103, `dict_flat.parquet`) and assert `extract(v2_dict.parquet)` logical rows == `extract(dict_flat.parquet)` logical rows for the shared columns/values (or simpler: assert v2_plain extract == the existing v1 plain fixture's extract for identical logical data). Document which equivalence you assert.

- [ ] **Step 6: e2e (6th, via the T1 helper)** — in `external_source_parquet_oracle.rs` add `refresh_v2_parquet_from_s3_fails_closed_and_state_intact` as a one-liner calling `run_fail_closed_parquet_e2e(V2_DICT_PARQUET_FIXTURE, "v2pq", "OBJ_V2PQ_KEYID", "OBJ_V2PQ_SECRET", "v2feed")` + the `V2_DICT_PARQUET_FIXTURE` static (mirrors the SP106 pattern, now via the shared helper). Existing 5 e2e fns unchanged (already one-liners post-T1).

- [ ] **Step 7:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet --test fixture_roundtrip 2>&1 | tail -8` (v2 roundtrips pass) and `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources-objstore --test external_source_parquet_oracle 2>&1 | tail -8` (6 pass).

- [ ] **Step 8:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green; existing oracles unchanged.

- [ ] **Step 9: Commit** (verify the 4 `.parquet` binaries staged):
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/tests/ crates/kesseldb-server/tests/external_source_parquet_oracle.rs && git commit -m "parquet: real pyarrow V2 fixtures (plain+dict+gzip+nullable) + V2 e2e via shared helper" && git push
```

---

### Task 5: Pentest pass (#189)

**Files:** Modify `crates/kessel-parquet/src/lib.rs` (`mod tests` — append/extend a `#[cfg(test)] mod pentest_v2`, matching the established per-slice pentest-module convention).

- [ ] **Step 1:** Add `catch_unwind` `nb`-style locks (helper mirroring the established `nb`/`no_panic_*` in the prior pentest modules) using `build_v2_plain_i64`-derived corruptions + hand-built V2 headers:
  - lying `def_len`/`rep_len`: patch the V2 header so `rep_len+def_len > compressed_size` → `Bad`, no OOB.
  - `rep_len > 0` → `Unsupported("REPEATED/nested V2 (repetition levels): OBJ-2c-5")`, no panic.
  - `uncompressed_page_size < rep_len+def_len` (vt underflow) → `Bad`.
  - a V2 OPTIONAL def-level value `> 1` → `Bad("v2 def-level exceeds max")`.
  - `num_nulls` inconsistent with the decoded def-levels (patch g2 num_nulls) → `Bad("v2 num_nulls vs def-levels mismatch")`.
  - uncompressed/`!is_compressed` values section length ≠ `vt` → `Bad`.
  - region shorter than `rep_len+def_len` (truncated V2 page) → `Bad`.
  - V2 + a corrupt Snappy/GZIP values section (build a V2 file with codec=GZIP, `is_compressed=true`, a garbage values section) → the codec's typed `Bad`/cap, no panic/OOM.
  - **positive correctness locks (assert exact `Ok`):** V2 PLAIN REQUIRED `[7,-2]`; V2 PLAIN OPTIONAL `[7,null,-2]` (scatter); V2 + dict `[7,null,7]`; a V2 file with codec=GZIP + `is_compressed=false` (raw values under a gzip chunk codec) decodes correctly; (the V2+gzip *compressed* compose is proven by the T4 real-pyarrow `v2_gzip.parquet` roundtrip — cite it).

- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet pentest_v2 2>&1 | tail -15` → all pass FAST (no hang/OOM). If a positive lock fails → BLOCKED (decoder bug, never weaken). If a hostile case panics/OOMs/hangs → BLOCKED (real vuln, exact detail).

- [ ] **Step 3:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green.

- [ ] **Step 4: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/lib.rs && git commit -m "parquet: pentest lock tests for DATA_PAGE_V2 decode (no panic/OOM)" && git push
```

---

### Task 6: Docs + gate reconciliation + memory (#190)

**Files:** Create `docs/superpowers/specs/2026-05-19-kesseldb-subproject107-v2pages.md`; modify `docs/STATUS.md`, `docs/USAGE.md`; modify (auto-memory, OUTSIDE repo, never git-add) `…\memory\project_kesseldb.md`, `…\memory\MEMORY.md`.

- [ ] **Step 1: Measure.** `cargo test --workspace --release 2>&1 | tail -25` → `<FINAL>`; `<DELTA> = <FINAL> − <BASELINE>` (Task 0's). FAILED=0, seed-7 green. `cargo tree -p kesseldb-server | grep -Ei "parquet|objstore|rustls|webpki"` none; `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty.

- [ ] **Step 2:** Read `docs/superpowers/specs/2026-05-19-kesseldb-subproject106-gzip.md` for the EXACT convention, then create `docs/superpowers/specs/2026-05-19-kesseldb-subproject107-v2pages.md` mirroring it: `# KesselDB — Subproject 107: OBJ-2c-3 Parquet V2 data pages`; `**Date:** 2026-05-19  **Status:** done — code + tests committed and passing.`; bare-backtick Builds-on (subproject 100–106 records) + Design + Plan lines; `---` separators. Sections:
   - **What shipped:** meta.rs field-8 DataPageHeaderV2; lib.rs `decode_data_page_v2` (raw level split before value-section decompression, NOT the whole-page seam) + shared `scatter_nulls` (V1-OPTIONAL & V2; V1 byte-identical); page_type==3 gate flip; supported matrix now flat REQUIRED|OPTIONAL × UNCOMPRESSED|Snappy|GZIP × PLAIN|dict × **V1 and V2** data pages.
   - **Resequencing & T1 disclosures:** OBJ-2c-2 zstd resequenced/deferred (rationale); the `run_fail_closed_parquet_e2e` extraction was a deliberate behavior-preserving refactor (the SP106-tracked follow-on), not an intended behavior change — all 5 prior e2e observable assertions preserved.
   - **Verification:** hand-built V2 KATs (PLAIN REQUIRED, PLAIN OPTIONAL `[7,null,-2]` with the NON-4-byte-prefixed def section, V2+dict) derived from parquet.thrift; real pyarrow `data_page_version='2.0'` fixtures (v2_plain/v2_dict/v2_gzip/v2_nullable, metadata-verified DataPageHeaderV2) round-trip via production extract(); V2-vs-V1 source-independence pin; e2e fail-closed (6th, via the shared helper); pentest (lying def/rep_len, rep_len>0→OBJ-2c-5, vt underflow, def>1, num_nulls cross-check, raw-len mismatch, truncated, V2+gzip corrupt + positive V2 plain/opt/dict/is_compressed=false locks).
   - **Honest gate accounting:** `<BASELINE>` → `<FINAL>` (+`<DELTA>`); NOT a zero-delta (SP100–106 stance; per-slice +DELTA authoritative per the tracked nit); T1 net-0 (refactor); kernel zero-dep; deps empty; seed-7 green; EXT/TLS/OBJ-1 oracles 2/1/1 unchanged; all V1 OBJ-2a/2b/2c-1 paths byte-unchanged.
   - **Deferred (OBJ-2c-2+):** zstd (OBJ-2c-2), INT96/DECIMAL (OBJ-2c-4), REPEATED/nested incl. V2 `rep_len>0` (OBJ-2c-5), lz4/brotli, >64MiB.
- [ ] **Step 3: STATUS.md** — insert SP107 row immediately AFTER the SP106 row (numeric order), matching the SP106 row format incl. gate `<BASELINE>→<FINAL> (+<DELTA>; …; not zero-delta)`, `Record:` backlink, clause: "DATA_PAGE_V2 now decoded (pyarrow data_page_version='2.0') for the existing flat REQUIRED|OPTIONAL × UNCOMPRESSED|Snappy|GZIP × PLAIN|dict matrix; raw-level-split V2 path, shared scatter_nulls (V1 byte-identical); OBJ-2c-2 zstd resequenced/deferred; T1 = behavior-preserving e2e-helper extraction. Still typed-Unsupported: zstd, INT96/DECIMAL, REPEATED/nested incl V2 rep-levels, >64MiB (OBJ-2c-2/4/5)."
- [ ] **Step 4: docs/USAGE.md** — append a §7f `> **OBJ-2c-3 (SP107):**` note (no overclaim) AND update the cumulative "### Parquet scope: what is currently supported (OBJ-2a → OBJ-2c-1)" table: retitle heading `(OBJ-2a → OBJ-2c-3)`; the Data-page-version row → `V1 and V2 (DATA_PAGE_V2)`; ensure the NOT-supported list still accurately keeps zstd/lz4/brotli, INT96/DECIMAL, REPEATED/nested (incl. V2 repetition levels), >64MiB; no §7f-vs-table contradiction; no stale tag.
- [ ] **Step 5:** `cargo test --workspace --release 2>&1 | tail -12` → `FAILED=0`, total == `<FINAL>`, seed-7 green.
- [ ] **Step 6: Commit docs**
```bash
cd /c/Users/ihass/KesselDB && git add docs/superpowers/specs/2026-05-19-kesseldb-subproject107-v2pages.md docs/STATUS.md docs/USAGE.md && git commit -m "docs: OBJ-2c-3 V2 data pages — subproject107 record + STATUS/USAGE cumulative-table + gate reconciliation" && git push
```
- [ ] **Step 7: Auto-memory (OUTSIDE repo — never git-add).** Append via Bash heredoc an SP107 block (substitute `<BASELINE>`/`<FINAL>`): summarise meta f8 + decode_data_page_v2 + shared scatter_nulls + page_type==3 flip; OBJ-2c-2 zstd resequenced/deferred; T1 behavior-preserving e2e-helper extraction (SP106-tracked); real pyarrow V2 fixtures; honest gate <BASELINE>→<FINAL>; kernel zero-dep + seed-7 + oracles 2/1/1; OBJ-2c arc 2/5 (gzip+V2 done). Then Read `/c/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/MEMORY.md`, find the `- [KesselDB](project_kesseldb.md) — …` line, Edit its trailing status clause to: `SP107 SHIPPED: OBJ-2c-3 DATA_PAGE_V2 decode (pyarrow data_page_version='2.0'; raw-level-split, shared scatter; V1 byte-identical). OBJ-2c arc 2/5. Open: OBJ-2c-2 zstd (resequenced) / OBJ-2c-4 INT96-DECIMAL / OBJ-2c-5 REPEATED-nested / OBJ-3 Iceberg / OBJ-4 listing / OBJ-5 STS-SAS / WASM / #75 SP-A scatter scan / seed-7 liveness / SQL-over-cluster`. Keep the line's existing prefix intact.
- [ ] **Step 8:** `cd /c/Users/ihass/KesselDB && git status --porcelain` EMPTY (no memory path, no stray logs; rm -f any test-output.log). Report DONE.

## Self-Review

**1. Spec coverage:** e2e-helper extraction → T1; meta field-8 DataPageHeaderV2 → T2; decode_data_page_v2 + shared scatter_nulls + page_type==3 flip + hand-built V2 KATs (REQUIRED/OPTIONAL/dict) + V2-vs-V1 pin → T3; real pyarrow V2 fixtures (plain/dict/gzip/nullable, metadata-verified DataPageHeaderV2) + roundtrips + 6th e2e via shared helper → T4; pentest (lying def/rep_len, rep_len>0, vt underflow, def>1, num_nulls cross-check, raw-len mismatch, truncated, V2+gzip corrupt + positive locks) → T5; honest gate + resequencing + T1-refactor disclosures + SP106-convention record + cumulative USAGE table → T6. All design sections mapped.

**2. Placeholder scan:** The T2 field-8 arm code is explicitly marked as illustrative-API-names with a directive to MIRROR the actual existing field-5/7 nested-struct mechanism (the StructReader method names + bool handling are codebase-specific — the implementer reads and matches them; the binding contract is the KAT assertions + the per-struct last_id bracketing). The T3 V2+dict builder is directed to compose the SP103 dict-page bytes + the V2 header (named existing sources). `<BASELINE>/<FINAL>/<DELTA>` are runtime-measured (T0/T6). No "handle edge cases"/"TBD"; the deterministic V2 KAT bytes (0x5c field-8 header, 0x15 0x06 type, the `[0x03,0x05]` non-prefixed def section) are fully spelled.

**3. Type consistency:** `decode_data_page_v2(&[u8],&meta::PageHeader,meta::Codec,meta::Type,u32,&[PqValue])->Result<Vec<PqValue>,PqError>`; `scatter_nulls(&[u64],Vec<PqValue>,usize)->Result<Vec<PqValue>,PqError>` used by both V1-OPTIONAL and V2; `run_fail_closed_parquet_e2e(&'static [u8],&str,&str,&str,&str)`; `PageHeader.v2_*` fields match T2↔T3; `PqError::{Bad,Unsupported}`/`PqValue::{I64,Bytes,Null}`/`meta::Codec::{Uncompressed,Snappy,Gzip,Other}` match the crate.

Plan is internally consistent and fully covers the OBJ-2c-3 design.
