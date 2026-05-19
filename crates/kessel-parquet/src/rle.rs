//! Apache Parquet RLE / bit-packing hybrid decoder.
//! Authority: parquet-format `Encodings.md`. Zero external deps.
//! Pure, bounds-checked: never panics / OOMs on hostile bytes.
#![allow(dead_code)]
// pub fns consumed by later OBJ-2b sub-slices (dictionary/levels).

use crate::PqError;

fn bad(s: &str) -> PqError {
    PqError::Bad(s.to_string())
}

/// Unsigned LEB128 varint at `data[*pos..]`; advances `*pos`.
/// Rejects a varint whose continuation runs past 64 bits (shift >= 64) as `Bad`.
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
            // bytes = groups * bit_width  (8 values * bit_width bits =
            // bit_width bytes per group of 8).
            let nbytes_u64 = groups
                .checked_mul(bit_width as u64)
                .ok_or_else(|| bad("rle bitpack byte count overflow"))?;
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
                // bit_width==0: no payload bytes exist; emit groups*8 zeros (chunk is empty here).
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
                        let bp = bitpos
                            .checked_add(k)
                            .ok_or_else(|| bad("rle bitpack bitpos overflow"))?;
                        let byte = *chunk
                            .get(bp / 8)
                            .ok_or_else(|| bad("rle bitpack index"))?;
                        let bit = (byte >> (bp % 8)) & 1;
                        v |= (bit as u64) << k;
                    }
                    bitpos = bitpos
                        .checked_add(bw)
                        .ok_or_else(|| bad("rle bitpack bitpos overflow"))?;
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
    // repeated-value width = ceil(bit_width/8) = ceil(3/8) = 1 byte = 0x05.
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

    // ── Independent grammar-faithful encoders (NOT the decoder under test) ──
    // Written directly from the parquet-format grammar; entirely separate
    // code path from decode_hybrid so a round-trip failure indicates a real
    // decoder bug.

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
}
