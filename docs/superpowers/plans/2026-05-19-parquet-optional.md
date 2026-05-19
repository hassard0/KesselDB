# OBJ-2b-4 Parquet OPTIONAL/nullable Columns Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decode flat OPTIONAL (nullable) Parquet columns via V1 definition levels, so `kessel-parquet::extract()` reads vanilla `pq.write_table(df)` output (pyarrow's default: OPTIONAL + dictionary + Snappy) with zero special flags — the capstone of the OBJ-2b arc.

**Architecture:** `meta.rs` learns whether the schema is flat (root group + leaves only). `lib.rs` computes per-leaf `max_def_level` (0 REQUIRED / 1 flat OPTIONAL), flips the OPTIONAL gate, adds a flat-schema guard, and a `decode_page` helper that — for OPTIONAL — decodes the def-level stream via the existing SP102 `rle::decode_level_v1`, decodes only the present values, and scatters `PqValue::Null` for absent rows. REQUIRED path byte-unchanged. No kessel-fetch/kessel-sql/server/kernel change.

**Tech Stack:** Rust (workspace edition), `#![forbid(unsafe_code)]`, zero external deps, existing `PqError`/`PqValue`, `rle::decode_level_v1` (SP102, already KAT'd, reused unchanged).

---

## Context for the implementer (read once)

`crates/kessel-parquet` modules: `thrift.rs`, `footer.rs`, `meta.rs`, `plain.rs`, `rle.rs`, `dict.rs`, `snappy.rs`, `lib.rs`. `Cargo.toml` `[dependencies]` is empty and MUST stay empty.

- `meta.rs`: `Repetition::{Required,Optional,Repeated,Other}`. `decode_schema_element` currently returns `Option<SchemaLeaf>` (`Some` for `num_children==0` leaf, `None` for any group — intermediate groups are silently dropped). `FileMetaData` holds `leaves: Vec<SchemaLeaf>` (each has `.repetition`).
- `lib.rs::extract()` ~line 226: `if leaf.repetition != meta::Repetition::Required { return Err(PqError::Unsupported("OPTIONAL/REPEATED columns: OBJ-2b".into())) }` — the gate to flip. It also resolves each wanted leaf and (further down) calls `read_chunk_values(file, cc, want_ptype)` which loops data pages and per page does roughly `match ph.dp_encoding { 0 => plain::decode_plain(&payload,wp,n), 2|8 => dict::resolve_dict_indices(&payload,&dict,n), _ => Unsupported }` where `payload` is the (post-Snappy) page bytes and `n = ph.dp_num_values`.
- `rle.rs` (SP102, KAT'd, **reused unchanged**): `pub fn decode_level_v1(data:&[u8], bit_width:u32, num_values:usize) -> Result<(Vec<u64>, usize), PqError>` — decodes a 4-byte-u32-LE-length-prefixed RLE/bit-packing-hybrid level stream; returns `(levels, total_bytes_consumed_including_the_4_byte_prefix)`.

**Parquet V1 OPTIONAL flat-leaf page payload** = `[def-levels: 4-byte-u32-LE-len-prefixed RLE-hybrid, bit_width=1, dp_num_values entries][values: only for rows with def-level==1; count = #(def==1)]`. No repetition-level bytes (flat non-REPEATED ⇒ max_rep_level=0). `dp_num_values` and `cc.num_values` are ROW counts **including nulls**. REQUIRED flat leaf (`max_def_level==0`) ⇒ no level bytes (the existing path).

**Discipline:** `#![forbid(unsafe_code)]` crate-wide. No unwrap/expect/panic/raw-index on input bytes (checked `get`/`checked_*`; only the statically-infallible 4-byte→`[u8;4]` `try_into().unwrap()`). KAT bytes below are hand-derived from parquet-format; a failing KAT means the CODE is wrong — never change a KAT; report BLOCKED if irreconcilable.

**Determinism / invariants gate — EVERY task (T0–T5):**
- `cargo test --workspace --release` → `FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` green.
- `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty; `cargo tree -p kesseldb-server` links no parquet/objstore/rustls/webpki.
- Existing oracles green unchanged: `external_source_oracle`(2), `external_source_tls_oracle`(1), `external_source_objstore_oracle`(1); ALL OBJ-2a/2b REQUIRED decode+gate tests green unchanged (REQUIRED path is byte-unchanged; existing files are flat).

**Commit discipline:** straight to `main`, no Co-Authored-By, no signing, message style like `git log -3`. `git push` after every task. Bash: prefix each call `cd /c/Users/ihass/KesselDB &&`; `cargo test --workspace --release` is long — allow 600000ms.

---

### Task 0: Determinism baseline (#171)

- [ ] **Step 1:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -30` — sum `passed` → `<BASELINE>` (expected **348**); `FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` in an `ok` result.
- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo tree -p kesseldb-server 2>&1 | grep -Ei "parquet|objstore|rustls|webpki"` → no output.
- [ ] **Step 3:** No commit. Report DONE with `OBJ-2b-4 baseline: <BASELINE> tests passing, FAILED=0, seed-7 green` + per-binary counts.

---

### Task 1: `meta.rs` flat-schema detection (#172)

**Files:** Modify `crates/kessel-parquet/src/meta.rs`

- [ ] **Step 1: Write the failing test** — add inside `meta.rs` `#[cfg(test)] mod tests` (has `uv`/`zz`):

```rust
#[test]
fn flat_schema_true_for_root_plus_leaves_false_for_nested_group() {
    // Helper: FileMetaData with `n_leaf` flat leaves under root, OR
    // with an extra intermediate group when `nested` is true.
    fn build(nested: bool) -> Vec<u8> {
        let mut b = Vec::new();
        b.push(0x15); uv(&mut b, zz(1)); // f1 version=1
        // f2 list<SchemaElement>: count = 2 (flat) or 3 (nested:
        //   root, group(num_children=1), leaf)
        let count = if nested { 3 } else { 2 };
        b.push(0x19); b.push(((count as u8) << 4) | 12); // list hdr: count<<4|STRUCT
        // schema[0] root group: name "schema", num_children = (nested?1:1)
        b.push(0x48); uv(&mut b, 6); b.extend_from_slice(b"schema");
        b.push(0x15); uv(&mut b, zz(1)); // f5 num_children=1
        b.push(0x00);
        if nested {
            // schema[1] intermediate GROUP "g": num_children=1, OPTIONAL
            b.push(0x25); uv(&mut b, zz(1)); // f3 repetition=OPTIONAL (delta 0->3=3? see note)
            // NOTE: emit a group element = has f5 num_children>0, no f1 type.
            // f4 name "g": (use field 4)
            b.push(0x00); // (placeholder — see Step-3 builder note)
        }
        // leaf "id": f1 type=INT64(2), f3 repetition=REQUIRED, f4 name
        b.push(0x15); uv(&mut b, zz(2));
        b.push(0x25); uv(&mut b, zz(0));
        b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
        b.push(0x00);
        b.push(0x16); uv(&mut b, zz(1)); // f3 num_rows=1
        b.push(0x19); b.push(0x1c);      // f4 list<RowGroup> 1
        b.push(0x19); b.push(0x1c);      // RG f1 list<ColumnChunk> 1
        b.push(0x3c);                    // ColumnChunk f3 ColumnMetaData
        b.push(0x15); uv(&mut b, zz(2)); // CMD f1 type=INT64
        b.push(0x19); b.push(0x15); uv(&mut b, zz(0)); // f2 enc [PLAIN]
        b.push(0x19); b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
        b.push(0x15); uv(&mut b, zz(0)); // f4 codec=UNCOMPRESSED
        b.push(0x16); uv(&mut b, zz(1)); // f5 num_values=1
        b.push(0x46); uv(&mut b, zz(4)); // f9 data_page_offset=4
        b.push(0x00); b.push(0x00);
        b.push(0x26); uv(&mut b, zz(1)); // RG f3 num_rows=1
        b.push(0x00); b.push(0x00);
        b
    }
    // NOTE TO IMPLEMENTER: the `nested` branch above is sketch-level.
    // Construct the nested-group element FAITHFULLY per parquet.thrift
    // SchemaElement {1:Type type(absent for groups), 3:RepetitionType,
    // 4:name, 5:num_children}. A GROUP element has NO field-1 type and
    // f5 num_children>0. Adjust root.num_children so the tree is
    // consistent (root->group->leaf). The assertions are what matter:
    let md_flat = FileMetaData::decode(&build(false)).expect("flat");
    assert!(md_flat.flat_schema, "root+leaves only ⇒ flat");
    let md_nested = FileMetaData::decode(&build(true)).expect("nested");
    assert!(!md_nested.flat_schema, "intermediate group ⇒ not flat");
}
```

(The `nested` byte construction is sketch-level on purpose — the
implementer must build a spec-faithful nested-group element: a
SchemaElement with **no field-1 type** and **f5 num_children = 1**,
placed between root and the leaf, with `root.num_children = 1` pointing
at the group and the group's `num_children = 1` pointing at the leaf.
Verify against parquet.thrift `SchemaElement`. The two `assert!`s are
the contract.)

- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet flat_schema_true 2>&1 | tail -8` → compile error (`no field flat_schema`).

- [ ] **Step 3: Implement.** In `meta.rs`:
  - Add `pub flat_schema: bool` to `FileMetaData`.
  - Introduce `enum SchemaNode { Leaf(SchemaLeaf), Group { num_children: i32 } }`. Change `decode_schema_element` to return `Result<SchemaNode, PqError>` (a `num_children > 0` element ⇒ `Group{num_children}`; a `num_children == 0` element with a type ⇒ `Leaf(SchemaLeaf{...})` exactly as the prior leaf decode).
  - In `FileMetaData::decode`, while iterating the schema list, collect nodes in order. After the loop compute:
    `flat_schema = nodes.len() >= 1 && matches!(nodes[0], SchemaNode::Group{..}) && nodes[1..].iter().all(|n| matches!(n, SchemaNode::Leaf(_))) && (if let SchemaNode::Group{num_children}=nodes[0] { num_children as usize == nodes.len()-1 } else { false })`.
    Push the `Leaf`s into `leaves` exactly as before (consumers of `leaves` unchanged). Set `md.flat_schema`.
  - Keep every other field/behaviour identical (existing tests build a flat root+1-leaf schema ⇒ `flat_schema==true`, and `leaves`/`row_groups`/`version`/`num_rows` decode unchanged).

- [ ] **Step 4:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet meta:: 2>&1 | tail -12` → the new test + ALL pre-existing meta tests pass (they're flat ⇒ `flat_schema==true`, and the `leaves` they assert are unchanged).

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, total ≥ baseline+1, seed-7 green.

- [ ] **Step 6: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/meta.rs && git commit -m "parquet: meta flat-schema detection (FileMetaData.flat_schema; SchemaNode group/leaf)" && git push
```

---

### Task 2: gate flip + `max_def_level` + `decode_page` null-scatter (#173)

**Files:** Modify `crates/kessel-parquet/src/lib.rs`

Before coding: READ `read_chunk_values` and the wanted-leaf resolution loop (the `lib.rs:226` gate, how `cc`/`want_ptype` reach `read_chunk_values`, and the per-page `match ph.dp_encoding` block). Preserve all REQUIRED behavior exactly.

- [ ] **Step 1: Write the failing tests + builders** — add inside `lib.rs` `mod tests`:

```rust
/// Flat OPTIONAL INT64 "id" file. `defs` (length = rows, values 0/1)
/// + `present_vals` (the non-null i64s, len == #(defs==1)). PLAIN.
/// Layout: [PAR1][page_hdr][page_payload][FileMetaData][mlen][PAR1].
/// page_payload = [u32 LE deflen][def hybrid][PLAIN i64 present_vals].
fn build_opt_plain_i64(defs: &[u8], present_vals: &[i64]) -> Vec<u8> {
    assert_eq!(
        present_vals.len(),
        defs.iter().filter(|&&d| d == 1).count()
    );
    let n = defs.len();
    // def-level hybrid: one bit-packed group of ceil(n/8)*8 (pad 0),
    // bit_width=1. header = (groups<<1)|1.
    let groups = ((n + 7) / 8).max(1) as u64;
    let mut def_hybrid = Vec::new();
    // varint(header)
    let mut h = (groups << 1) | 1;
    loop { let b=(h&0x7f) as u8; h>>=7;
        if h==0 { def_hybrid.push(b); break } else { def_hybrid.push(b|0x80) } }
    let nbytes = (groups as usize) * 1; // bit_width 1 → 1 byte/group
    let mut bits = vec![0u8; nbytes];
    for (i, &d) in defs.iter().enumerate() {
        if d == 1 { bits[i / 8] |= 1 << (i % 8); }
    }
    def_hybrid.extend_from_slice(&bits);
    let mut payload = Vec::new();
    payload.extend_from_slice(&(def_hybrid.len() as u32).to_le_bytes());
    payload.extend_from_slice(&def_hybrid);
    for v in present_vals { payload.extend_from_slice(&v.to_le_bytes()); }
    let psz = payload.len() as i64;

    let mut hdr = Vec::new();
    hdr.push(0x15); uv(&mut hdr, zz(0));        // f1 type=DATA_PAGE(0)
    hdr.push(0x15); uv(&mut hdr, zz(psz));      // f2 uncompressed_page_size
    hdr.push(0x15); uv(&mut hdr, zz(psz));      // f3 compressed_page_size
    hdr.push(0x2c);                             // f5 DataPageHeader (delta 3->5=2, struct)
    hdr.push(0x15); uv(&mut hdr, zz(n as i64)); // g1 num_values = rows (incl nulls)
    hdr.push(0x15); uv(&mut hdr, zz(0));        // g2 encoding=PLAIN(0)
    hdr.push(0x00); hdr.push(0x00);

    let data_page_offset: i64 = 4;
    let mut m = Vec::new();
    m.push(0x15); uv(&mut m, zz(2));            // f1 version=2
    m.push(0x19); m.push(0x2c);                 // f2 list<SchemaElement> 2
    m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
    m.push(0x15); uv(&mut m, zz(1));            // root num_children=1
    m.push(0x00);
    m.push(0x15); uv(&mut m, zz(2));            // leaf f1 type=INT64
    m.push(0x25); uv(&mut m, zz(1));            // f3 repetition=OPTIONAL(1)
    m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
    m.push(0x00);
    m.push(0x16); uv(&mut m, zz(n as i64));     // f3 num_rows = rows
    m.push(0x19); m.push(0x1c);                 // f4 list<RowGroup> 1
    m.push(0x19); m.push(0x1c);                 // RG f1 list<ColumnChunk> 1
    m.push(0x3c);                               // ColumnChunk f3 ColumnMetaData
    m.push(0x15); uv(&mut m, zz(2));            // CMD f1 type=INT64
    m.push(0x19); m.push(0x15); uv(&mut m, zz(0)); // f2 enc [PLAIN]
    m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
    m.push(0x15); uv(&mut m, zz(0));            // f4 codec=UNCOMPRESSED
    m.push(0x16); uv(&mut m, zz(n as i64));     // f5 num_values = rows
    m.push(0x46); uv(&mut m, zz(data_page_offset));
    m.push(0x00); m.push(0x00);
    m.push(0x26); uv(&mut m, zz(n as i64));     // RG f3 num_rows = rows
    m.push(0x00); m.push(0x00);

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
fn extract_decodes_optional_int64_with_nulls() {
    // [7, null, -2]: defs [1,0,1], present [7,-2].
    let f = build_opt_plain_i64(&[1, 0, 1], &[7, -2]);
    let rows = extract(&f, &["id"]).expect("opt");
    assert_eq!(rows, vec![
        vec![PqValue::I64(7)], vec![PqValue::Null], vec![PqValue::I64(-2)],
    ]);
}

#[test]
fn extract_optional_all_null_page() {
    let f = build_opt_plain_i64(&[0, 0], &[]);
    assert_eq!(extract(&f, &["id"]).expect("allnull"),
        vec![vec![PqValue::Null], vec![PqValue::Null]]);
}

#[test]
fn extract_optional_all_present_page() {
    let f = build_opt_plain_i64(&[1, 1], &[7, -2]);
    assert_eq!(extract(&f, &["id"]).expect("allpresent"),
        vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]);
}

#[test]
fn extract_rejects_repeated_obj2c() {
    // REPEATED(2) leaf still Unsupported (OBJ-2c).
    let f = build_parquet_file(0, 0, 2, false); // (enc,codec,repetition=REPEATED,dicthdr)
    assert!(matches!(extract(&f, &["id"]), Err(PqError::Unsupported(_))),
        "REPEATED must be Unsupported (OBJ-2c)");
}
```

Then DELETE the old `extract_rejects_optional_repetition` test (it
asserted OPTIONAL is rejected — intentionally superseded by
`extract_decodes_optional_int64_with_nulls` + `extract_rejects_repeated_obj2c`).
Leave every OTHER OBJ-2a/2b test untouched.

Also add an OPTIONAL+dictionary builder + test (defs then a dict-index
data-page body for the present count). REUSE the SP103
`build_dict_int64_file_with_dict_offset`-style dictionary-page
construction; prepend the def-level prefix to the data page, set the
leaf repetition OPTIONAL(1), data page `dp_encoding=PLAIN_DICTIONARY(2)`,
dp_num_values = rows. For `[7, null, 7]`: defs `[1,0,1]`, dict `[7]`,
data-page body after the def-stream = `[0x01 (bit_width=1)][0x03 (1
bit-packed group hdr)][0x00 (indices 0,0,0… ⇒ all index 0)]`; expect
`extract` → `[[I64(7)],[Null],[I64(7)]]`. Name it
`extract_decodes_optional_dict_int64_with_nulls`. (Spell the full
builder explicitly in the test, mirroring `build_opt_plain_i64` +
the SP103 dict-page bytes; do not leave it as prose.)

- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet extract_decodes_optional_int64_with_nulls 2>&1 | tail -8` → FAIL (OPTIONAL gate rejects).

- [ ] **Step 3: Add a nested-schema reject test** (uses the meta flat-schema flag from T1). Build a file whose schema has an intermediate group (root→group→leaf) — reuse the T1 nested builder shape — and assert `extract` → `Unsupported`:
```rust
#[test]
fn extract_rejects_nested_schema_obj2c() {
    // A non-flat schema (root → intermediate group → leaf) must be
    // Unsupported("nested schema: OBJ-2c") regardless of repetition.
    let f = /* build a root+group+leaf file (faithful SchemaElement:
              group has no f1 type, f5 num_children=1; root
              num_children=1) with one INT64 leaf + one PLAIN page */;
    assert!(matches!(extract(&f, &["id"]), Err(PqError::Unsupported(_))),
        "nested schema must be Unsupported (OBJ-2c)");
}
```
(Construct the file faithfully — same nested SchemaElement shape as
T1's `build(true)`; if T1's nested builder is reusable, factor a
shared `mod tests` helper.)

- [ ] **Step 4: Implement.** In `lib.rs`:
  - **Gate + max_def_level** (replace the `lib.rs:226` block). Per wanted leaf:
    ```rust
    if !md.flat_schema {
        return Err(PqError::Unsupported("nested schema: OBJ-2c".into()));
    }
    let max_def_level: u32 = match leaf.repetition {
        meta::Repetition::Required => 0,
        meta::Repetition::Optional => 1,
        meta::Repetition::Repeated =>
            return Err(PqError::Unsupported("REPEATED columns: OBJ-2c".into())),
        meta::Repetition::Other(_) =>
            return Err(PqError::Unsupported("unknown repetition: OBJ-2c".into())),
    };
    ```
    Thread `max_def_level` into `read_chunk_values` (add a param;
    it's per wanted column). (`md` is the decoded `FileMetaData` in
    scope in `extract()`; pass `md.flat_schema` / compute per-leaf as
    above before calling `read_chunk_values`, and pass `max_def_level`
    down.)
  - **`decode_page` helper** (free fn). Move the existing per-page
    `match ph.dp_encoding { 0 => plain::decode_plain(&payload,wp,n),
    2|8 => dict::resolve_dict_indices(&payload,&dict,n), _ =>
    Unsupported }` into it for the `max_def_level == 0` arm
    (byte-identical), and add the OPTIONAL arm:
    ```rust
    fn decode_page(
        payload: &[u8],
        dp_encoding: i32,
        wp: meta::Type,
        n: usize,            // dp_num_values = ROW count incl nulls
        max_def_level: u32,  // 0 REQUIRED, 1 flat OPTIONAL
        dict: &[PqValue],
    ) -> Result<Vec<PqValue>, PqError> {
        if max_def_level == 0 {
            return match dp_encoding {
                0 => plain::decode_plain(payload, wp, n),
                2 | 8 => dict::resolve_dict_indices(payload, dict, n),
                _ => Err(PqError::Unsupported(
                    "data page encoding (DELTA/BYTE_STREAM_SPLIT): OBJ-2c".into())),
            };
        }
        // max_def_level == 1: flat OPTIONAL
        let (defs, consumed) = rle::decode_level_v1(payload, 1, n)?;
        if defs.len() != n {
            return Err(PqError::Bad("def-level count != num_values".into()));
        }
        for &d in &defs {
            if d > 1 {
                return Err(PqError::Bad("definition level exceeds max".into()));
            }
        }
        let present = defs.iter().filter(|&&d| d == 1).count();
        let body = payload
            .get(consumed..)
            .ok_or_else(|| PqError::Bad("def-level consumed past payload".into()))?;
        let vals = match dp_encoding {
            0 => plain::decode_plain(body, wp, present)?,
            2 | 8 => dict::resolve_dict_indices(body, dict, present)?,
            _ => return Err(PqError::Unsupported(
                "data page encoding (DELTA/BYTE_STREAM_SPLIT): OBJ-2c".into())),
        };
        if vals.len() != present {
            return Err(PqError::Bad("value/def-level count mismatch".into()));
        }
        let mut out = Vec::with_capacity(n);
        let mut it = vals.into_iter();
        for &d in &defs {
            if d == 1 {
                out.push(it.next().ok_or_else(|| {
                    PqError::Bad("value/def-level count mismatch".into())
                })?);
            } else {
                out.push(PqValue::Null);
            }
        }
        Ok(out)
    }
    ```
  - In `read_chunk_values`'s data-page loop, replace the inline
    `match ph.dp_encoding {...}` with
    `let vals = decode_page(&payload, ph.dp_encoding, want_ptype,
    n, max_def_level, &dict)?;` (keep the dict-page decode, the
    page_payload Snappy/Cow handling, the page_type / loop-termination
    / overshoot / undershoot logic byte-identical). The dictionary
    **page** itself is REQUIRED-style PLAIN (no levels) — unchanged.

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet 2>&1 | tail -25` → ALL pass: the new optional tests (incl. all-null/all-present/dict), `extract_rejects_repeated_obj2c`, `extract_rejects_nested_schema_obj2c`, AND every existing OBJ-2a/2b test (`extract_golden_int64_two_rows`, `extract_decodes_dictionary_int64`, `extract_decodes_snappy_plain_int64`, `extract_snappy_and_uncompressed_identical`, `extract_rejects_gzip_codec_obj2c`, `extract_rejects_delta_encoding`, `extract_rejects_schema_chunk_type_mismatch`, `extract_rejects_missing_column`, the dict-offset/truncated locks); the deleted `extract_rejects_optional_repetition` is gone; `FAILED=0`.

- [ ] **Step 6:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green. Record measured total.

- [ ] **Step 7: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/lib.rs && git commit -m "parquet: flat OPTIONAL via V1 def-levels (decode_page null-scatter) + flat-schema guard (intended OBJ-2a test change)" && git push
```

---

### Task 3: Real pyarrow nullable fixtures + e2e + source-independence pin (#174)

**Files:** Create `crates/kessel-parquet/tests/fixtures/nullable.parquet`, `.../nullable_plain.parquet`; modify `.../README.md`, `crates/kessel-parquet/tests/fixture_roundtrip.rs`, `crates/kesseldb-server/tests/external_source_parquet_oracle.rs`.

- [ ] **Step 1: Generate (real pyarrow 24.0.0):**
```bash
cd /c/Users/ihass/KesselDB && python -c "
import pyarrow as pa, pyarrow.parquet as pq
# VANILLA default: nullable columns, pyarrow default dict+snappy.
t = pa.table({'id': pa.array([7,7,None,-2,100], type=pa.int64()),
              's':  pa.array(['a',None,'b','c','a'], type=pa.large_utf8())})
pq.write_table(t,'crates/kessel-parquet/tests/fixtures/nullable.parquet',
               version='1.0', data_page_version='1.0')   # NO use_dictionary/compression flags = pyarrow defaults
pq.write_table(t,'crates/kessel-parquet/tests/fixtures/nullable_plain.parquet',
               use_dictionary=False, compression=None,
               version='1.0', data_page_version='1.0')
print('wrote nullable.parquet + nullable_plain.parquet rows=5')
"
```
Expected `wrote ... rows=5`. If pyarrow fails → STOP, BLOCKED.

- [ ] **Step 2: Metadata verify (OPTIONAL; vanilla one SNAPPY+dict):**
```bash
cd /c/Users/ihass/KesselDB && python -c "
import pyarrow.parquet as pq
for f in ['nullable','nullable_plain']:
  pf=pq.ParquetFile(f'crates/kessel-parquet/tests/fixtures/{f}.parquet'); rg=pf.metadata.row_group(0)
  print(f, pf.schema_arrow, [(rg.column(i).compression, rg.column(i).encodings) for i in range(pf.metadata.num_columns)])
  t=pq.read_table(f'crates/kessel-parquet/tests/fixtures/{f}.parquet'); print(' ', t.column('id').to_pylist(), t.column('s').to_pylist())
"
```
Expected: schema fields **nullable** (OPTIONAL); `nullable.parquet` columns compression `SNAPPY` + `PLAIN_DICTIONARY` (pyarrow default); `nullable_plain.parquet` `UNCOMPRESSED` + `PLAIN`; rows `id=[7,7,None,-2,100]`, `s=['a',None,'b','c','a']`. If schema is NOT nullable or `nullable.parquet` isn't SNAPPY/dict → STOP, BLOCKED (report metadata).

- [ ] **Step 3: README** — append to `crates/kessel-parquet/tests/fixtures/README.md`:
```markdown
## nullable.parquet / nullable_plain.parquet (OBJ-2b-4)

Regenerate:

    python -c "import pyarrow as pa, pyarrow.parquet as pq; \
    t=pa.table({'id':pa.array([7,7,None,-2,100],type=pa.int64()),'s':pa.array(['a',None,'b','c','a'],type=pa.large_utf8())}); \
    pq.write_table(t,'crates/kessel-parquet/tests/fixtures/nullable.parquet',version='1.0',data_page_version='1.0'); \
    pq.write_table(t,'crates/kessel-parquet/tests/fixtures/nullable_plain.parquet',use_dictionary=False,compression=None,version='1.0',data_page_version='1.0')"

Real pyarrow 24.0.0. `nullable.parquet` = VANILLA default (OPTIONAL +
dictionary + Snappy, with NULLs). `nullable_plain.parquet` = OPTIONAL +
PLAIN + UNCOMPRESSED, with NULLs. V1, flat schema.
Expected rows: id=[7,7,null,-2,100]; s=["a",null,"b","c","a"].
```

- [ ] **Step 4: Roundtrip test** — READ `fixture_roundtrip.rs` for its import/const convention; add a test loading both, calling `kessel_parquet::extract(&bytes,&["id","s"])`, asserting:
```rust
vec![
  vec![I64(7),  Bytes(b"a".to_vec())],
  vec![I64(7),  Null],
  vec![Null,    Bytes(b"b".to_vec())],
  vec![I64(-2), Bytes(b"c".to_vec())],
  vec![I64(100),Bytes(b"a".to_vec())],
]
```
(using the file's existing `kessel_parquet::PqValue::{I64,Bytes,Null}` path/import style; assert for BOTH fixtures).

- [ ] **Step 5: e2e** — READ `crates/kesseldb-server/tests/external_source_parquet_oracle.rs`; add `refresh_nullable_parquet_from_s3_fails_closed_and_state_intact` mirroring the SP104 case via the same `tls_stub_with_fixture` harness pointed at `nullable.parquet` (fail-closed, no router fixture-trust bypass; differentiate env-var/shard-tag/source-name like prior cases).

- [ ] **Step 6: Source-format-independence pin** (test-only, no production change). In the kesseldb-server test crate (where kessel-fetch coerce + json + parquet meet — read an existing test there to find the right harness; if a JSON external-source oracle test exists, mirror it), add a test that the same logical 1-column nullable table materializes byte-identical field bytes whether the source row is a JSON `null` or the Parquet OPTIONAL def==0 row. If wiring a full dual-source oracle is disproportionate, instead add a focused kessel-fetch unit/integration test asserting `pq_to_cell(PqValue::Null)` → the same `json::Cell` a JSON `null` produces and that `coerce::to_field_bytes` yields identical bytes for both — whichever is cleanly expressible WITHOUT changing production code. Document in the report exactly which form you used and why it's a valid source-independence pin.

- [ ] **Step 7:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet --test fixture_roundtrip 2>&1 | tail -8` (nullable roundtrips pass) and `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources-objstore --test external_source_parquet_oracle 2>&1 | tail -8` (SP101+103+104 + new nullable case pass) and the source-indep pin test.

- [ ] **Step 8:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green; existing oracles unchanged.

- [ ] **Step 9: Commit** (verify both `.parquet` staged):
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/tests/ crates/kesseldb-server/tests/ && git commit -m "parquet: real pyarrow nullable fixtures (vanilla default + plain) + nullable e2e + source-indep pin" && git push
```

---

### Task 4: Pentest pass (#175)

**Files:** Modify `crates/kessel-parquet/src/lib.rs` (`mod tests` — append a `#[cfg(test)] mod pentest_optional` OR add into the existing pentest module if one exists at lib level; check and match the established placement).

- [ ] **Step 1:** Add lock tests (each `std::panic::catch_unwind`, asserting no panic + the expected typed result). Use `build_opt_plain_i64` from T2 and small hand-built corruptions:
  - def-level stream truncated (chop the payload after the length prefix) → `Err(Bad)`.
  - lying 4-byte length prefix (prefix says 999 but few bytes follow) → `Err(Bad)`.
  - a def-level value `> 1`: build a def hybrid with bit_width 2 encoding a `2`, declare it as the level stream → `extract` → `Err(Bad("definition level exceeds max"))`.
  - value section shorter than `present` (defs say 3 present, only 2 i64s) → `Err(Bad)` (value/def-level count mismatch).
  - OPTIONAL + dict with an out-of-range index → `Err(Bad)`.
  - non-flat schema (root→group→leaf) → `Err(Unsupported)` (no panic).
  - **positive correctness locks (assert `Ok`, NOT error):** all-null page → all `PqValue::Null`; all-present page → no nulls; a mixed `[1,0,1,1,0]` scatter → exact placement.
  Each wrapped: `let r = std::panic::catch_unwind(|| extract(&f,&["id"])); assert!(r.is_ok()); assert!(matches!(r.unwrap(), <expected>));`

- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet pentest 2>&1 | tail -15` → all pass FAST (no hang/OOM). If a positive lock (all-null/all-present/mixed) fails, the scatter is wrong — fix `decode_page`, never the test (BLOCKED if irreconcilable).

- [ ] **Step 3:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green.

- [ ] **Step 4: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/lib.rs && git commit -m "parquet: pentest lock tests for OPTIONAL def-level decode + null scatter (no panic/OOM)" && git push
```

---

### Task 5: Docs + gate reconciliation + memory (#176)

**Files:** Create `docs/superpowers/specs/2026-05-19-kesseldb-subproject105-optional.md`; modify `docs/STATUS.md`, `docs/USAGE.md`; modify (auto-memory, OUTSIDE repo, never git-add) `…\memory\project_kesseldb.md`, `…\memory\MEMORY.md`.

- [ ] **Step 1: Measure.** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -25` → `<FINAL>`; `<DELTA> = <FINAL> − 348`. `FAILED=0`, seed-7 green. `cargo tree -p kesseldb-server | grep -Ei "parquet|objstore|rustls|webpki"` none; `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty.

- [ ] **Step 2: Read `docs/superpowers/specs/2026-05-19-kesseldb-subproject104-snappy.md`** for the exact convention, then create `docs/superpowers/specs/2026-05-19-kesseldb-subproject105-optional.md` mirroring it EXACTLY: `# KesselDB — Subproject 105: OBJ-2b-4 Parquet OPTIONAL/nullable columns`; `**Date:** 2026-05-19  **Status:** done — code + tests committed and passing.`; bare-backtick-path `Builds on:` list (subproject 100/101/102/103/104 record paths) + `Design document:` + `Plan document:` lines; `---` separators. Sections:
  - **What shipped:** flat OPTIONAL via V1 def-levels; `meta.rs` flat-schema detection; `lib.rs` `max_def_level` + gate flip + `decode_page` null-scatter reusing `rle::decode_level_v1`; REQUIRED path byte-unchanged. Supported matrix now = flat schema, REQUIRED or OPTIONAL, UNCOMPRESSED|Snappy, PLAIN|dictionary, V1 = **vanilla `pq.write_table(df)`**.
  - **Latent OBJ-2a flat-schema tightening (honest disclosure):** the flat-schema guard now rejects nested/intermediate-group schemas with `Unsupported("nested schema: OBJ-2c")`; OBJ-2a silently flattened them (a leaf under a nested group would mis-compute levels). Validated non-self-referentially: all real pyarrow fixtures are flat and still round-trip. (Same disclosure spirit as SP104's field-ID fix.)
  - **Verification:** spec KATs (OPTIONAL PLAIN `[7,null,-2]`, all-null, all-present, OPTIONAL+dict; `rle::decode_level_v1` itself SP102-KAT'd, here the null-scatter); real pyarrow vanilla `nullable.parquet` (OPTIONAL+dict+Snappy with NULLs) + `nullable_plain.parquet` round-trip; source-format-independence pin (Parquet-OPTIONAL-null FieldKind == JSON-null FieldKind); e2e fail-closed; pentest.
  - **Intended behavior change (reviewed — NOT a regression):** `extract_rejects_optional_repetition` → `extract_decodes_optional_int64_with_nulls` + `extract_rejects_repeated_obj2c` + `extract_rejects_nested_schema_obj2c`; all other OBJ-2a/2b tests unchanged.
  - **Honest gate accounting:** 348 → `<FINAL>` (+`<DELTA>`) — new meta/optional/fixture/pentest tests; NOT a zero-delta (SP100–104 stance); kernel zero-dep; deps empty; seed-7 green; EXT/TLS/OBJ-1 oracles 2/1/1 unchanged; REQUIRED path byte-unchanged.
  - **Deferred (OBJ-2c):** REPEATED/repetition levels, nested/optional groups (`max_def_level>1`), LIST/MAP; gzip/zstd/lz4/brotli, INT96/DECIMAL, V2 pages, >64 MiB Snappy.

- [ ] **Step 3: STATUS.md** — read it, insert the SP105 row immediately AFTER the SP104 row (numeric order), matching the SP104 row format incl. gate numbers `348→<FINAL> (+<DELTA>; ...; not zero-delta)`, `Record:` backlink, and a clause: `vanilla pq.write_table(df) (flat OPTIONAL+dict+Snappy) now reads with zero flags; also tightened a latent OBJ-2a nested-schema flatten → Unsupported(nested schema: OBJ-2c). Still typed-Unsupported: REPEATED/nested + gzip/zstd/INT96/V2/>64MiB (OBJ-2c).`

- [ ] **Step 4: docs/USAGE.md** — append a §7f `> **OBJ-2b-4 (SP105):**` note (vanilla pyarrow default — flat REQUIRED or OPTIONAL, UNCOMPRESSED|Snappy, PLAIN|dictionary, V1 — now supported; REPEATED/nested + gzip/zstd/INT96/V2/>64MiB still → OBJ-2c; no overclaim) AND update the cumulative "Parquet scope: what is currently supported" table (the one SP104 retitled): Column repetition row `REQUIRED flat columns only` → `REQUIRED or OPTIONAL flat columns (nullable; V1 definition levels)`; ensure the NOT-supported list still accurately lists REPEATED/repetition-levels, nested/optional groups, gzip/zstd/lz4/brotli, INT96/DECIMAL, V2, >64MiB Snappy. Retitle the table heading to `### Parquet scope: what is currently supported (OBJ-2a → OBJ-2b-4)`.

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -12` → `FAILED=0`, total == `<FINAL>`, seed-7 green.

- [ ] **Step 6: Commit docs**
```bash
cd /c/Users/ihass/KesselDB && git add docs/superpowers/specs/2026-05-19-kesseldb-subproject105-optional.md docs/STATUS.md docs/USAGE.md && git commit -m "docs: OBJ-2b-4 OPTIONAL — subproject105 record (+flat-schema tightening disclosure) + STATUS/USAGE cumulative-table + gate reconciliation" && git push
```

- [ ] **Step 7: Auto-memory (OUTSIDE repo — never git-add).** Append via Bash heredoc (do NOT full-Read then Edit):
```bash
cat >> "/c/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/project_kesseldb.md" <<'EOF'

## SP105 (2026-05-19) — OBJ-2b-4 Parquet OPTIONAL/nullable (capstone)
extract() now reads vanilla pq.write_table(df) (flat OPTIONAL + dict +
Snappy, V1, with NULLs) — zero special flags. meta.rs flat-schema
detection (FileMetaData.flat_schema; SchemaNode group/leaf); lib.rs
per-leaf max_def_level + OPTIONAL gate flip + flat-schema guard +
decode_page null-scatter reusing SP102 rle::decode_level_v1 (REQUIRED
path byte-unchanged). Real pyarrow nullable.parquet + nullable_plain
fixtures + e2e fail-closed + source-format-independence pin
(Parquet-null FieldKind == JSON-null). Honest disclosure: flat-schema
guard tightens a latent OBJ-2a nested-schema flatten →
Unsupported(nested schema: OBJ-2c). Intended change: optional-reject
test → positive+repeated/nested rejects. Honest gate 348→<FINAL>.
Kernel zero-dep + seed-7 + EXT/TLS/OBJ-1 oracles 2/1/1 unchanged.
OBJ-2b arc COMPLETE. Next: OBJ-2c (REPEATED/nested/LIST/MAP, gzip/
zstd, INT96/DECIMAL, V2 pages, >64MiB Snappy) / OBJ-3 Iceberg /
OBJ-4 listing / OBJ-5 STS-SAS / WASM / #75 SP-A.
EOF
```
(substitute `<FINAL>`). Then update the KesselDB line in
`/c/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/MEMORY.md`
(Read to find the exact `- [KesselDB](project_kesseldb.md) — …` line,
Edit it) so the trailing status clause becomes:
`SP105 SHIPPED: OBJ-2b-4 flat OPTIONAL/nullable — vanilla pq.write_table(df) now reads with zero flags (OBJ-2b arc COMPLETE) + tightened latent OBJ-2a nested-schema flatten. Open: OBJ-2c (REPEATED/nested/LIST/MAP, gzip/zstd, INT96/DECIMAL, V2, >64MiB Snappy) / OBJ-3 Iceberg / OBJ-4 listing / OBJ-5 STS-SAS / WASM / #75 SP-A scatter scan / seed-7 liveness / SQL-over-cluster`.
Keep the rest of the line's prefix intact.

- [ ] **Step 8:** `cd /c/Users/ihass/KesselDB && git status --porcelain` EMPTY (no memory path, no stray logs; `rm -f` any test-output.log). Report DONE with 348/`<FINAL>`/`<DELTA>`, FAILED, seed-7, deps-clean, the disclosures present, docs commit SHA, memory updated & not git-added, clean tree.

---

## Self-Review

**1. Spec coverage:** flat_schema detection → T1; gate flip + max_def_level + flat-schema guard + decode_page null-scatter (REQUIRED byte-unchanged) → T2; OPTIONAL PLAIN + dict + all-null + all-present KATs + intended test split → T2; real pyarrow vanilla nullable + plain fixtures + e2e + source-independence pin → T3; pentest (truncation/lying-len/def>1/count-mismatch/dict-OOB/non-flat + positive locks) → T4; honest gate + flat-schema-tightening disclosure + SP104-convention record + cumulative USAGE table → T5. All design sections mapped.

**2. Placeholder scan:** No "TBD"/"handle edge cases". `<BASELINE>/<FINAL>/<DELTA>` runtime-measured (defined T0/T5). KAT bytes hand-derived & concrete (`[0x02,0,0,0,0x03,0x05]` def-stream etc.). The T1 nested-builder and T2 nested-reject/dict builders carry explicit construction guidance + the binding assertions (the SchemaElement faithfulness is specified, not vague) — the implementer builds them spec-faithfully; this is acceptable scaffolding, not a placeholder, because the contract (assertions) is exact.

**3. Type consistency:** `decode_page(&[u8], i32, meta::Type, usize, u32, &[PqValue]) -> Result<Vec<PqValue>, PqError>`; `rle::decode_level_v1(&[u8],u32,usize)->Result<(Vec<u64>,usize),PqError>` (SP102, unchanged); `FileMetaData.flat_schema: bool`; `SchemaNode::{Leaf,Group}`; `Repetition::{Required,Optional,Repeated,Other}`; `PqValue::{I64,Bytes,Null}` — all consistent across tasks and match the existing crate.

Plan is internally consistent and fully covers the OBJ-2b-4 design.
