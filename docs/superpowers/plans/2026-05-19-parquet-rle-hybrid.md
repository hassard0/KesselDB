# OBJ-2b-1 RLE/Bit-Packing Hybrid Decoder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pure, bounds-checked Parquet RLE/bit-packing-hybrid decoder (`crates/kessel-parquet/src/rle.rs`) — the shared primitive for dictionary indices and definition/repetition levels — KAT-pinned to the published `parquet-format/Encodings.md` grammar, with no wiring or support-matrix change.

**Architecture:** One new private module `rle.rs` in the existing zero-external-dependency `kessel-parquet` crate, exposing `decode_hybrid` (framing-agnostic) and `decode_level_v1` (the 4-byte-u32-LE-length-prefixed V1 level wrapper). Nothing else in the workspace changes; the crate is still compiled only by `kessel-fetch`'s `object-store` feature, so the deterministic kernel + seed-7 corpus are byte-untouched.

**Tech Stack:** Rust (workspace edition), `#![forbid(unsafe_code)]`, zero external dependencies, the existing `PqError` type.

---

## Context for the implementer (read once)

You are extending `kessel-parquet`, a hand-written Parquet reader. Existing modules: `thrift.rs`, `footer.rs`, `meta.rs`, `plain.rs`, `lib.rs`. **You only create `rle.rs` and add one `mod rle;` line to `lib.rs`.** Do **not** touch `extract()`, the support-matrix gate, `kessel-fetch`, `kessel-sql`, or the server. `kessel-parquet/Cargo.toml` `[dependencies]` MUST stay empty.

**The encoding** (authority: Apache `parquet-format` `Encodings.md` — *the* independent reference; do **not** derive expected bytes from your own decoder):

- A stream is a sequence of *runs*. Each run starts with an unsigned LEB128 **varint header**.
- `header & 1 == 1` → **bit-packed run**: `groups = header >> 1` is the number of groups-of-8; the run yields `groups * 8` values; the values occupy `groups * bit_width` bytes, packed **LSB-of-stream-first** (value 0's least-significant bit is stream bit 0).
- `header & 1 == 0` → **RLE run**: `run_length = header >> 1`; followed by the repeated value in `ceil(bit_width/8)` bytes, **little-endian**.
- `bit_width == 0` is legal: every value is `0` and **no value bytes are consumed** (the run still has its varint header).
- The caller asks for exactly `num_values`. Decode whole runs until `>= num_values` produced, then `truncate(num_values)` (bit-packed runs over-produce; the padding is discarded).

**Discipline (matches `plain.rs`/`thrift.rs`):** `#![forbid(unsafe_code)]` is crate-wide already. No `unwrap`/`expect`/`panic`/raw indexing on input bytes — every read is a checked `get(..)` / `checked_*`. (The single allowed exception, matching `plain.rs:87`, is `<[u8]>::try_into().unwrap()` on a slice **already** length-checked to exactly 4 bytes for `u32::from_le_bytes` — it is statically infallible.)

**Determinism / invariants gate — run on EVERY task (T0–T5):**
- `cargo test --workspace --release` → `FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` green.
- `kessel-parquet/Cargo.toml` `[dependencies]` empty; default `cargo tree` links no parquet/objstore/rustls.
- Existing oracles green unchanged: `external_source_oracle` (2), `external_source_tls_oracle` (1), `external_source_objstore_oracle` (1), and all pre-existing `kessel-parquet` tests.

**Commit discipline:** straight to `main`, no Co-Authored-By, no signing, message style like `git log -3` (e.g. `parquet: ...`, `docs: ...`). `git push` after every task.

---

### Task 0: Determinism baseline (#151)

**Files:** none (measurement only).

- [ ] **Step 1: Build & run the full suite (release)**

Run: `cargo test --workspace --release 2>&1 | tail -40`
Expected: `FAILED=0` (some suites print `test result: ok. N passed`). Note the **total passed count across all binaries** and that `large_seed_corpus_is_deterministic_and_converges` is in the `ok` set.

- [ ] **Step 2: Record the baseline number**

In your task report write: `OBJ-2b-1 baseline: <TOTAL> tests passing, FAILED=0, seed-7 green` where `<TOTAL>` is the summed `passed` count. This is the honest before-number Task 5 reconciles against.

- [ ] **Step 3: Confirm default-build dependency cleanliness**

Run: `cargo tree -p kesseldb-server 2>&1 | grep -Ei "parquet|objstore|rustls|webpki" | head` (PowerShell: `cargo tree -p kesseldb-server | Select-String "parquet|objstore|rustls|webpki"`)
Expected: **no output** (default build links none of these).

- [ ] **Step 4: Commit**

No code changed; nothing to commit. Report DONE with the baseline number.

---

### Task 1: `decode_hybrid` + spec KATs (#152)

**Files:**
- Create: `crates/kessel-parquet/src/rle.rs`
- Modify: `crates/kessel-parquet/src/lib.rs` (add `mod rle;` next to the other `mod` lines)

- [ ] **Step 1: Add the module declaration to `lib.rs`**

In `crates/kessel-parquet/src/lib.rs`, the existing block is:

```rust
mod thrift;
mod footer;
mod meta;
mod plain;
```

Change it to:

```rust
mod thrift;
mod footer;
mod meta;
mod plain;
mod rle;
```

- [ ] **Step 2: Write the failing test file** (`crates/kessel-parquet/src/rle.rs`, tests only for now)

Create `crates/kessel-parquet/src/rle.rs` with ONLY this content first (the `tests` module references items that don't exist yet, so it must fail to compile — that is the "red"):

```rust
//! Apache Parquet RLE / bit-packing hybrid decoder.
//! Authority: parquet-format `Encodings.md`. Zero external deps.
//! Pure, bounds-checked: never panics / OOMs on hostile bytes.
#![allow(dead_code)]

use crate::PqError;

fn bad(s: &str) -> PqError {
    PqError::Bad(s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    // KAT 1 — the canonical parquet-format Encodings.md example:
    // bit_width=3, one bit-packed group of 8 values 0..=7.
    // header = (number_of_groups_of_8 << 1) | 1 = (1<<1)|1 = 0x03.
    // LSB-of-stream-first packing of 0,1,2,3,4,5,6,7 (3 bits each):
    //   byte0 = 0b1000_1000 = 0x88
    //   byte1 = 0b1100_0110 = 0xC6
    //   byte2 = 0b1111_1010 = 0xFA
    #[test]
    fn kat_bitpacked_0_to_7_width3() {
        let stream = [0x03u8, 0x88, 0xC6, 0xFA];
        let v = decode_hybrid(&stream, 3, 8).expect("decode");
        assert_eq!(v, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    // KAT 2 — RLE run: value 5 repeated 8 times, bit_width=3.
    // header = varint(run_len << 1) = varint(8<<1) = varint(16) = 0x10.
    // repeated-value width = ceil(3/8) = 1 byte = 0x05.
    #[test]
    fn kat_rle_run_value5_x8_width3() {
        let stream = [0x10u8, 0x05];
        let v = decode_hybrid(&stream, 3, 8).expect("decode");
        assert_eq!(v, vec![5, 5, 5, 5, 5, 5, 5, 5]);
    }

    // KAT 3 — bit_width == 0: RLE header varint(4<<1)=varint(8)=0x08,
    // NO value byte; four zeros.
    #[test]
    fn kat_bitwidth0_rle_four_zeros() {
        let stream = [0x08u8];
        let v = decode_hybrid(&stream, 0, 4).expect("decode");
        assert_eq!(v, vec![0, 0, 0, 0]);
    }

    // KAT 4 — mixed: RLE(value=5, run_len=4, bw=3) then the bit-packed
    // 0..=7 group. RLE header varint(4<<1)=0x08, value 0x05; then
    // 0x03,0x88,0xC6,0xFA.
    #[test]
    fn kat_mixed_rle_then_bitpacked() {
        let stream = [0x08u8, 0x05, 0x03, 0x88, 0xC6, 0xFA];
        let v = decode_hybrid(&stream, 3, 12).expect("decode");
        assert_eq!(v, vec![5, 5, 5, 5, 0, 1, 2, 3, 4, 5, 6, 7]);
    }

    // KAT 5 — over-production truncation: same stream, ask for 10.
    // The bit-packed run yields 8 but only 6 are needed → truncate.
    #[test]
    fn kat_overproduction_truncates() {
        let stream = [0x08u8, 0x05, 0x03, 0x88, 0xC6, 0xFA];
        let v = decode_hybrid(&stream, 3, 10).expect("decode");
        assert_eq!(v, vec![5, 5, 5, 5, 0, 1, 2, 3, 4, 5]);
    }

    // KAT 6 — RLE repeated-value wide width: bit_width=17 →
    // ceil(17/8)=3 bytes little-endian. value = 100000 = 0x01_86A0
    // → LE bytes [0xA0,0x86,0x01]. run_len=2 → header varint(2<<1)=0x04.
    #[test]
    fn kat_rle_wide_value_width17() {
        let stream = [0x04u8, 0xA0, 0x86, 0x01];
        let v = decode_hybrid(&stream, 17, 2).expect("decode");
        assert_eq!(v, vec![100_000, 100_000]);
    }
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `cargo test -p kessel-parquet rle:: 2>&1 | tail -20`
Expected: compile error — `cannot find function decode_hybrid in this scope`.

- [ ] **Step 4: Implement `decode_hybrid` (and the private `uvarint`)**

Insert this **above** the `#[cfg(test)] mod tests` block in `crates/kessel-parquet/src/rle.rs`:

```rust
/// Unsigned LEB128 varint at `data[*pos..]`; advances `*pos`.
/// Rejects > 10 continuation groups (cannot fit u64) as `Bad`.
fn uvarint(data: &[u8], pos: &mut usize) -> Result<u64, PqError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let b = *data
            .get(*pos)
            .ok_or_else(|| bad("rle varint truncated"))?;
        *pos += 1;
        if shift >= 64 {
            return Err(bad("rle varint too long"));
        }
        result |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

/// Decode exactly `num_values` from a Parquet RLE/bit-packing-hybrid
/// stream of fixed `bit_width` (0..=64). Values returned as `u64`;
/// the caller narrows (dictionary index / definition / repetition
/// level). Consumes only the bytes the runs require; bit-packed
/// over-production past `num_values` is discarded. Never panics /
/// OOM-aborts on hostile input — returns `PqError::Bad`.
pub fn decode_hybrid(
    data: &[u8],
    bit_width: u32,
    num_values: usize,
) -> Result<Vec<u64>, PqError> {
    if bit_width > 64 {
        return Err(bad("rle bit_width > 64"));
    }
    // OOM bound (matches plain.rs:35 stance): `num_values` is the
    // caller's expected count, itself bounded upstream by the page
    // header's dp_num_values (SP101 Task-12 capped). We NEVER reserve
    // from a run-length/group-count read out of the (attacker) header.
    let mut out: Vec<u64> = Vec::with_capacity(num_values);
    let val_bytes = ((bit_width as usize) + 7) / 8; // ceil; 0 when bw==0
    let mut pos = 0usize;

    while out.len() < num_values {
        let header = uvarint(data, &mut pos)?;
        if header & 1 == 1 {
            // ── bit-packed run ──
            let groups = header >> 1;
            let total_vals = groups
                .checked_mul(8)
                .ok_or_else(|| bad("rle bitpack value count overflow"))?;
            let nbits = groups
                .checked_mul(bit_width as u64)
                .ok_or_else(|| bad("rle bitpack bit count overflow"))?
                .checked_mul(8)
                .ok_or_else(|| bad("rle bitpack bit count overflow"))?;
            // bytes = groups * bit_width  (8 values * bit_width bits =
            // bit_width bytes per group of 8). Recompute precisely:
            let nbytes_u64 = groups
                .checked_mul(bit_width as u64)
                .ok_or_else(|| bad("rle bitpack byte count overflow"))?;
            let _ = nbits; // documented relation: nbits == nbytes*8
            let nbytes = usize::try_from(nbytes_u64)
                .map_err(|_| bad("rle bitpack byte count range"))?;
            let end = pos
                .checked_add(nbytes)
                .ok_or_else(|| bad("rle bitpack position overflow"))?;
            let chunk = data
                .get(pos..end)
                .ok_or_else(|| bad("rle bitpack run truncated"))?;
            pos = end;
            let tv = usize::try_from(total_vals)
                .map_err(|_| bad("rle bitpack value count range"))?;
            if bit_width == 0 {
                for _ in 0..tv {
                    if out.len() >= num_values {
                        break;
                    }
                    out.push(0);
                }
            } else {
                let bw = bit_width as usize;
                let mut bitpos = 0usize;
                for _ in 0..tv {
                    if out.len() >= num_values {
                        break;
                    }
                    let mut v: u64 = 0;
                    for k in 0..bw {
                        let bp = bitpos + k;
                        let byte = *chunk
                            .get(bp / 8)
                            .ok_or_else(|| bad("rle bitpack index"))?;
                        let bit = (byte >> (bp % 8)) & 1;
                        v |= (bit as u64) << k;
                    }
                    bitpos += bw;
                    out.push(v);
                }
            }
        } else {
            // ── RLE run ──
            let run_len = header >> 1;
            let value: u64 = if bit_width == 0 {
                0
            } else {
                let end = pos
                    .checked_add(val_bytes)
                    .ok_or_else(|| bad("rle value position overflow"))?;
                let vb = data
                    .get(pos..end)
                    .ok_or_else(|| bad("rle repeated value truncated"))?;
                pos = end;
                let mut v: u64 = 0;
                for (i, &b) in vb.iter().enumerate() {
                    v |= (b as u64) << (8 * i as u32);
                }
                v
            };
            // Push at most what is still needed: a giant run_len is
            // legal and simply satisfies num_values (no OOM — we never
            // allocate run_len).
            let mut remaining = run_len;
            while remaining > 0 && out.len() < num_values {
                out.push(value);
                remaining -= 1;
            }
        }
    }
    out.truncate(num_values);
    Ok(out)
}
```

- [ ] **Step 5: Run KATs to verify they pass**

Run: `cargo test -p kessel-parquet rle::tests 2>&1 | tail -20`
Expected: `test result: ok. 6 passed` (all six KATs green).

- [ ] **Step 6: Full determinism gate**

Run: `cargo test --workspace --release 2>&1 | tail -20`
Expected: `FAILED=0`, total ≥ baseline + 6, `large_seed_corpus_is_deterministic_and_converges` green.

- [ ] **Step 7: Commit**

```bash
git add crates/kessel-parquet/src/rle.rs crates/kessel-parquet/src/lib.rs
git commit -m "parquet: RLE/bit-packing hybrid decoder (decode_hybrid) + spec KATs"
git push
```

---

### Task 2: `decode_level_v1` length-prefixed wrapper (#153)

**Files:**
- Modify: `crates/kessel-parquet/src/rle.rs`

- [ ] **Step 1: Write the failing test**

Add inside `mod tests` in `crates/kessel-parquet/src/rle.rs`:

```rust
// KAT 7 — V1 level stream framing: a 4-byte u32 LE length prefix
// followed by exactly `length` hybrid bytes. Body = RLE(value=1,
// run_len=4, bw=1): header varint(4<<1)=0x08, value byte 0x01
// (ceil(1/8)=1). Body length = 2 → prefix [0x02,0,0,0].
// decode_level_v1 returns four 1s and total_consumed = 4 + 2 = 6.
#[test]
fn kat_decode_level_v1_prefix_and_consumed() {
    let data = [0x02u8, 0x00, 0x00, 0x00, 0x08, 0x01];
    let (levels, consumed) =
        decode_level_v1(&data, 1, 4).expect("decode");
    assert_eq!(levels, vec![1, 1, 1, 1]);
    assert_eq!(consumed, 6);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p kessel-parquet rle::tests::kat_decode_level_v1 2>&1 | tail -10`
Expected: compile error — `cannot find function decode_level_v1`.

- [ ] **Step 3: Implement `decode_level_v1`**

Add immediately after `decode_hybrid` in `crates/kessel-parquet/src/rle.rs`:

```rust
/// V1 definition/repetition level stream: a 4-byte little-endian
/// `u32` length prefix, then exactly that many bytes of hybrid
/// `<encoded-data>`. Decodes `num_values` levels of `bit_width` and
/// returns `(levels, total_consumed)` where `total_consumed` includes
/// the 4-byte prefix (so the caller can advance to the value section).
pub fn decode_level_v1(
    data: &[u8],
    bit_width: u32,
    num_values: usize,
) -> Result<(Vec<u64>, usize), PqError> {
    let lb = data
        .get(0..4)
        .ok_or_else(|| bad("rle level length prefix truncated"))?;
    // lb is exactly 4 bytes (get(0..4) succeeded) → try_into is
    // statically infallible; same pattern as plain.rs:87.
    let len = u32::from_le_bytes(lb.try_into().unwrap()) as usize;
    let end = 4usize
        .checked_add(len)
        .ok_or_else(|| bad("rle level length overflow"))?;
    let body = data
        .get(4..end)
        .ok_or_else(|| bad("rle level body truncated"))?;
    let levels = decode_hybrid(body, bit_width, num_values)?;
    Ok((levels, end))
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p kessel-parquet rle::tests::kat_decode_level_v1 2>&1 | tail -10`
Expected: `test result: ok. 1 passed`.

- [ ] **Step 5: Full determinism gate**

Run: `cargo test --workspace --release 2>&1 | tail -20`
Expected: `FAILED=0`, total ≥ baseline + 7, seed-7 green.

- [ ] **Step 6: Commit**

```bash
git add crates/kessel-parquet/src/rle.rs
git commit -m "parquet: V1 length-prefixed level wrapper (decode_level_v1)"
git push
```

---

### Task 3: Non-self-referential round-trip property test (#154)

**Files:**
- Modify: `crates/kessel-parquet/src/rle.rs`

This proves the decoder against an **independent encoder** written straight from the grammar (a separate code path — not the decoder).

- [ ] **Step 1: Write the failing test (encoder + round trip)**

Add inside `mod tests` in `crates/kessel-parquet/src/rle.rs`:

```rust
// Independent grammar-faithful encoders (NOT the decoder under test).
fn enc_uvarint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            break;
        } else {
            out.push(b | 0x80);
        }
    }
}

/// Encode `vals` as a single bit-packed run (caller pads to a
/// multiple of 8). bit_width 1..=32. LSB-of-stream-first.
fn enc_bitpacked(vals: &[u64], bit_width: u32) -> Vec<u8> {
    assert!(vals.len() % 8 == 0 && bit_width >= 1 && bit_width <= 32);
    let groups = (vals.len() / 8) as u64;
    let mut out = Vec::new();
    enc_uvarint(&mut out, (groups << 1) | 1);
    let nbytes = vals.len() * bit_width as usize / 8;
    let mut bytes = vec![0u8; nbytes];
    let mut bitpos = 0usize;
    for &val in vals {
        for k in 0..bit_width as usize {
            let bit = ((val >> k) & 1) as u8;
            if bit == 1 {
                bytes[(bitpos + k) / 8] |= 1 << ((bitpos + k) % 8);
            }
        }
        bitpos += bit_width as usize;
    }
    out.extend_from_slice(&bytes);
    out
}

/// Encode a single RLE run of `value` repeated `run_len` times.
fn enc_rle(value: u64, run_len: u64, bit_width: u32) -> Vec<u8> {
    let mut out = Vec::new();
    enc_uvarint(&mut out, run_len << 1);
    let vb = ((bit_width as usize) + 7) / 8;
    for i in 0..vb {
        out.push(((value >> (8 * i as u32)) & 0xff) as u8);
    }
    out
}

#[test]
fn roundtrip_bitpacked_all_widths() {
    // Deterministic LCG (no external rand crate — zero-dep).
    let mut state: u64 = 0x1234_5678_9abc_def0;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state >> 11
    };
    for bw in 1u32..=32 {
        for &count in &[8usize, 16, 64, 256] {
            let mask = if bw == 64 { u64::MAX } else { (1u64 << bw) - 1 };
            let vals: Vec<u64> =
                (0..count).map(|_| next() & mask).collect();
            let stream = enc_bitpacked(&vals, bw);
            let got = decode_hybrid(&stream, bw, count).expect("decode");
            assert_eq!(got, vals, "bw={bw} count={count}");
        }
    }
}

#[test]
fn roundtrip_rle_all_widths() {
    for bw in 1u32..=32 {
        let mask = (1u64 << bw) - 1;
        let value = 0xA5A5_A5A5_A5A5_A5A5u64 & mask;
        for &run in &[1u64, 7, 100, 1000] {
            let stream = enc_rle(value, run, bw);
            let got =
                decode_hybrid(&stream, bw, run as usize).expect("decode");
            assert_eq!(got, vec![value; run as usize], "bw={bw}");
        }
    }
}
```

- [ ] **Step 2: Run to verify it passes (decoder already exists from T1)**

Run: `cargo test -p kessel-parquet rle::tests::roundtrip 2>&1 | tail -10`
Expected: `test result: ok. 2 passed`. (These are characterization tests over the existing decoder; they pass once written. If either fails, the **decoder** has a bug — fix `decode_hybrid`, not the encoder.)

- [ ] **Step 3: Full determinism gate**

Run: `cargo test --workspace --release 2>&1 | tail -20`
Expected: `FAILED=0`, total ≥ baseline + 9, seed-7 green.

- [ ] **Step 4: Commit**

```bash
git add crates/kessel-parquet/src/rle.rs
git commit -m "parquet: independent-encoder round-trip property tests for rle"
git push
```

---

### Task 4: Pentest lock tests (#155)

**Files:**
- Modify: `crates/kessel-parquet/src/rle.rs`

Every case is wrapped in `std::panic::catch_unwind` (proves no panic / no OOM-unwind) and asserted to be **well-behaved**: either `Ok(v)` with `v.len() == num_values` (a huge-but-valid run legitimately satisfies the request) **or** `Err(PqError::Bad(_))`. It must NEVER panic, stack-overflow, or OOM-abort.

- [ ] **Step 1: Write the pentest module**

Append at the **end** of `crates/kessel-parquet/src/rle.rs` (a sibling of `mod tests`, after it):

```rust
// ── PENTEST PASS — adversarial lock tests ─────────────────────────
//
// RLE/bit-packing streams inside a Parquet object are
// operator-declared-source-controlled = attacker-influenceable. The
// run-length / group-count are varint header values up to ~2^63.
// Each case proves: no panic / no OOM / no stack-overflow, and a
// well-formed Result (Ok of exactly num_values, OR Err(Bad)).
#[cfg(test)]
mod pentest {
    use super::*;

    fn enc_uvarint(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                break;
            } else {
                out.push(b | 0x80);
            }
        }
    }

    fn well_behaved(data: &[u8], bw: u32, nv: usize) {
        let owned = data.to_vec();
        let r = std::panic::catch_unwind(move || {
            decode_hybrid(&owned, bw, nv)
        });
        assert!(r.is_ok(), "must NOT panic/OOM-unwind");
        match r.unwrap() {
            Ok(v) => assert_eq!(
                v.len(),
                nv,
                "Ok must return exactly num_values"
            ),
            Err(PqError::Bad(_)) => {}
            Err(other) => {
                panic!("unexpected error variant: {other:?}")
            }
        }
    }

    #[test]
    fn rle_header_run_len_max_no_oom() {
        // RLE header with a near-u64::MAX run length, bit_width=1.
        // header = run_len<<1; encode the max varint payload.
        let mut s = Vec::new();
        enc_uvarint(&mut s, u64::MAX & !1); // even → RLE; huge run_len
        s.push(0x01); // 1 repeated-value byte (ceil(1/8))
        // num_values small: must return Ok([1;4]) WITHOUT allocating
        // run_len elements.
        well_behaved(&s, 1, 4);
    }

    #[test]
    fn bitpacked_groups_max_rejected_before_alloc() {
        // header = (groups<<1)|1 with groups ≈ u64::MAX/2 → the
        // groups*8 / groups*bit_width checked_mul overflows → Bad,
        // BEFORE any unpack/allocation.
        let mut s = Vec::new();
        enc_uvarint(&mut s, u64::MAX | 1); // odd → bit-packed
        well_behaved(&s, 8, 4);
    }

    #[test]
    fn truncated_final_bitpacked_group() {
        // 1 group of 8 @ bit_width=3 needs 3 bytes; supply only 1.
        let s = [0x03u8, 0x88];
        well_behaved(&s, 3, 8);
    }

    #[test]
    fn truncated_rle_repeated_value() {
        // RLE run_len=2 bit_width=17 → needs 3 value bytes; supply 1.
        let s = [0x04u8, 0xA0];
        well_behaved(&s, 17, 2);
    }

    #[test]
    fn bit_width_64_tiny_slice() {
        // bit_width 64: bit-packed group needs 64 bytes; RLE needs 8.
        // A 2-byte slice cannot satisfy either → Bad, no panic.
        well_behaved(&[0x03u8, 0x00], 64, 8);
        well_behaved(&[0x02u8, 0x00], 64, 1);
    }

    #[test]
    fn bit_width_65_rejected() {
        let r = decode_hybrid(&[0x02u8], 65, 1);
        assert!(matches!(r, Err(PqError::Bad(_))));
    }

    #[test]
    fn empty_slice_num_values_positive() {
        well_behaved(&[], 3, 4);
        let r = decode_hybrid(&[], 3, 4);
        assert!(matches!(r, Err(PqError::Bad(_))));
    }

    #[test]
    fn decode_level_v1_oversized_prefix() {
        // u32 length prefix = 0xFFFF_FFFF but only a few body bytes.
        let data = [0xFFu8, 0xFF, 0xFF, 0xFF, 0x08, 0x01];
        let owned = data.to_vec();
        let r = std::panic::catch_unwind(move || {
            decode_level_v1(&owned, 1, 4)
        });
        assert!(r.is_ok(), "must NOT panic");
        assert!(matches!(
            r.unwrap(),
            Err(PqError::Bad(_))
        ));
    }
}
```

- [ ] **Step 2: Run the pentest module to verify it passes**

Run: `cargo test -p kessel-parquet rle::pentest 2>&1 | tail -15`
Expected: `test result: ok. 8 passed`.

If `rle_header_run_len_max_no_oom` hangs or OOMs: the implementation is allocating/iterating from the header run-length. The fix is the Task-1 design — reservation is `Vec::with_capacity(num_values)` and the RLE push loop is bounded by `out.len() < num_values`; do not "fix" the test.

- [ ] **Step 3: Full determinism gate**

Run: `cargo test --workspace --release 2>&1 | tail -20`
Expected: `FAILED=0`, total ≥ baseline + 17, seed-7 green.

- [ ] **Step 4: Commit**

```bash
git add crates/kessel-parquet/src/rle.rs
git commit -m "parquet: pentest lock tests for rle hybrid decoder (no panic/OOM)"
git push
```

---

### Task 5: Docs + gate reconciliation + memory (#156)

**Files:**
- Create: `docs/superpowers/specs/2026-05-19-kesseldb-subproject102-rle.md`
- Modify: `STATUS.md`, `docs/USAGE.md` (one honest line each)
- Modify (auto-memory, outside repo): `C:\Users\ihass\.claude\projects\C--Users-ihass--local-bin\memory\project_kesseldb.md` and `...\MEMORY.md`

- [ ] **Step 1: Measure the final number**

Run: `cargo test --workspace --release 2>&1 | tail -20`
Record the final total passing count. Compute `delta = final - baseline` (Task 0). It MUST equal the number of new `rle` tests added (6 KAT + 1 level + 2 round-trip + 8 pentest = 17). This is the **honest** rise — `kessel-parquet` is an existing workspace member so its new unit tests run under `cargo test --workspace`; this is NOT a zero-delta (same corrected stance as SP100/SP101).

- [ ] **Step 2: Write the internal record**

Create `docs/superpowers/specs/2026-05-19-kesseldb-subproject102-rle.md`:

```markdown
# Subproject 102 — OBJ-2b-1: RLE/bit-packing hybrid decoder

**Date:** 2026-05-19
**Design:** docs/superpowers/specs/2026-05-19-parquet-rle-hybrid-design.md
**Plan:** docs/superpowers/plans/2026-05-19-parquet-rle-hybrid.md
**Builds on:** subproject 97/98/99/100/101

## What shipped

`crates/kessel-parquet/src/rle.rs` — a pure, bounds-checked Apache
Parquet RLE/bit-packing-hybrid decoder, KAT-pinned to the published
`parquet-format/Encodings.md` grammar (the independent authority):

- `decode_hybrid(data, bit_width, num_values) -> Result<Vec<u64>, PqError>`
  — framing-agnostic hybrid `<encoded-data>` decode (bit-packed +
  RLE runs, LSB-of-stream-first packing, bit_width 0..=64,
  over-production truncation).
- `decode_level_v1(data, bit_width, num_values) -> Result<(Vec<u64>, usize), PqError>`
  — the V1 4-byte-u32-LE-length-prefixed level-stream wrapper;
  returns levels + total bytes consumed (incl. prefix).

This is the shared primitive the next sub-slices consume. **No wiring
and no support-matrix gate changed in this slice** — dictionary,
Snappy, and OPTIONAL columns are still rejected with the exact same
typed `Unsupported` errors as OBJ-2a, until OBJ-2b-2/3/4 flip them.

## Verification

- KATs hand-derived from `parquet-format/Encodings.md` (canonical
  bit-packed 0..=7 width-3 example = `[0x03,0x88,0xC6,0xFA]`; RLE
  run; bit_width=0; mixed; wide-value width-17; V1 prefix framing).
- Independent-encoder round-trip (separate code path) over bit
  widths 1..=32 — non-self-referential.
- Pentest: catch_unwind lock tests prove no panic/OOM/stack-overflow
  on hostile headers (run_len≈2^63, groups≈2^63, truncated runs,
  bit_width=64 tiny slice, bit_width=65, oversized V1 prefix, empty
  slice) — typed `PqError::Bad` or exactly-num_values `Ok`.

## Honest gate accounting

`kessel-parquet` is an existing workspace member; its tests run under
`cargo test --workspace`. Default-build total: **<BASELINE> → <FINAL>**
(+<DELTA>), entirely the 17 new `rle` module tests. NOT a zero-delta
(same corrected stance as SP100/SP101). The deterministic kernel pulls
no new external dependency; `kessel-parquet/Cargo.toml` `[dependencies]`
stays empty; default `cargo tree` links no parquet/objstore/rustls;
`large_seed_corpus_is_deterministic_and_converges` green; existing
EXT/TLS/OBJ-1 oracles (2/1/1) unchanged.

## Deferred (next OBJ-2b sub-slices)

- OBJ-2b-2: dictionary page + index resolution (flips dict gate).
- OBJ-2b-3: Snappy block decompression (flips Snappy gate).
- OBJ-2b-4: OPTIONAL/def-levels + nullable wiring + support-matrix
  flips + pyarrow-default fixtures + e2e.
```

Replace `<BASELINE>`, `<FINAL>`, `<DELTA>` with the measured numbers.

- [ ] **Step 2b: Verify default-build dependency cleanliness**

Run: `cargo tree -p kesseldb-server 2>&1 | grep -Ei "parquet|objstore|rustls|webpki"` (PowerShell: `... | Select-String ...`)
Expected: no output. Record this in the record.

- [ ] **Step 3: Add one honest line to STATUS.md**

Open `STATUS.md`, find the Parquet/OBJ-2a line, and add immediately after it:

```markdown
- OBJ-2b-1 (SP102): pure RLE/bit-packing-hybrid decoder primitive
  (`kessel-parquet::rle`) landed — KAT-pinned to parquet-format
  Encodings.md, pentested. No support-matrix change yet: dictionary /
  Snappy / OPTIONAL still typed-Unsupported until OBJ-2b-2/3/4.
```

- [ ] **Step 4: Add one honest line to docs/USAGE.md**

Open `docs/USAGE.md`, find the Parquet section, and add:

```markdown
> **OBJ-2b in progress:** the RLE/bit-packing-hybrid primitive is
> implemented (SP102) but not yet wired. Until OBJ-2b-2/3/4 ship,
> `FORMAT PARQUET` still requires PLAIN-encoded, UNCOMPRESSED,
> REQUIRED columns (pyarrow `use_dictionary=False, compression=None`).
```

- [ ] **Step 5: Determinism gate (docs-only, must still hold)**

Run: `cargo test --workspace --release 2>&1 | tail -20`
Expected: `FAILED=0`, total == final from Step 1, seed-7 green.

- [ ] **Step 6: Commit docs**

```bash
git add docs/superpowers/specs/2026-05-19-kesseldb-subproject102-rle.md STATUS.md docs/USAGE.md
git commit -m "docs: OBJ-2b-1 rle primitive — subproject102 record + STATUS/USAGE + gate reconciliation"
git push
```

- [ ] **Step 7: Update auto-memory**

Append an SP102 entry to `C:\Users\ihass\.claude\projects\C--Users-ihass--local-bin\memory\project_kesseldb.md` (use a Bash heredoc append — the file is large; do NOT full-Read then Edit):

Content of the appended block:

```
## SP102 (2026-05-19) — OBJ-2b-1 RLE/bit-packing hybrid decoder
New crates/kessel-parquet/src/rle.rs: decode_hybrid + decode_level_v1,
pure/zero-dep, KAT-pinned to parquet-format Encodings.md, pentested
(no panic/OOM on ~2^63 run/group headers). NO wiring / NO support-
matrix flip this slice (dictionary/Snappy/OPTIONAL still typed-
Unsupported). Honest gate: <BASELINE>→<FINAL> (+17 new rle tests,
existing-member rise, not zero-delta). Kernel zero-dep + seed-7 green
+ EXT/TLS/OBJ-1 oracles 2/1/1 unchanged. Next: OBJ-2b-2 dictionary
(consumes decode_hybrid for the index stream) / OBJ-2b-3 Snappy /
OBJ-2b-4 OPTIONAL+nullable+pyarrow-default fixtures+e2e.
```

Then update the KesselDB line in `C:\Users\ihass\.claude\projects\C--Users-ihass--local-bin\memory\MEMORY.md` to note `SP102 SHIPPED: OBJ-2b-1 rle primitive (decode_hybrid/decode_level_v1, KAT'd+pentested, no gate flip yet)` and the open backlog `OBJ-2b-2 dict / OBJ-2b-3 Snappy / OBJ-2b-4 OPTIONAL+fixtures+e2e / OBJ-2c / OBJ-3 / OBJ-4 / OBJ-5 / WASM / #75 SP-A`.

- [ ] **Step 8: Report DONE** with the final gate numbers and the measured delta == 17.

---

## Self-Review

**1. Spec coverage** (design → task):
- `decode_hybrid` framing-agnostic, bit_width 0..=64, over-production truncation → Task 1 ✓
- LSB-of-stream-first packing, canonical `[0x03,0x88,0xC6,0xFA]` KAT → Task 1 KAT 1 ✓
- RLE run, ceil(bit_width/8) LE repeated value, bit_width=0 → Task 1 KATs 2/3/6 ✓
- `decode_level_v1` u32-LE prefix, returns `(Vec<u64>, total_consumed)` incl. prefix → Task 2 ✓
- Non-self-referential cross-check (independent encoder) → Task 3 ✓
- Pentest: run_len/groups ≈2^63, truncations, bw=64/65, oversized prefix, empty slice; no panic/OOM; reservation bounded by num_values not header → Task 4 ✓
- Zero-dep, no wiring, no gate flip, `#![forbid(unsafe_code)]`, checked reads → Context + Tasks 1/2 ✓
- Honest gate reconciliation (not zero-delta), seed-7, oracles unchanged, internal record mirroring SP99/100/101 → Task 5 ✓

**2. Placeholder scan:** No "TBD"/"handle edge cases"/"similar to". `<BASELINE>/<FINAL>/<DELTA>` are runtime-measured values explicitly defined in Task 0 / Task 5 Step 1 (not placeholders — they are the honest measurement the task computes). All code blocks are complete.

**3. Type consistency:** `decode_hybrid(&[u8], u32, usize) -> Result<Vec<u64>, PqError>` and `decode_level_v1(&[u8], u32, usize) -> Result<(Vec<u64>, usize), PqError>` are used identically in every task and the design. `bad(&str) -> PqError`, `uvarint(&[u8], &mut usize) -> Result<u64, PqError>` consistent. `PqError::Bad` variant matches the existing crate enum (`lib.rs:27`).

Plan is internally consistent and fully covers the OBJ-2b-1 design.
