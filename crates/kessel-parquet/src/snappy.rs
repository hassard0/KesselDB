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
    // OOM-safe: expected_len <= SNAPPY_MAX_DECOMP verified above;
    // never reserve from an attacker-controlled stream count.
    let mut out: Vec<u8> = Vec::with_capacity(expected_len);
    while let Some(&tag) = src.get(pos) {
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
                        .ok_or_else(|| bad("snappy pos overflow"))?;
                    len = v;
                }
                len = len
                    .checked_add(1)
                    .ok_or_else(|| bad("snappy literal len overflow"))?;
                let end = pos
                    .checked_add(len)
                    .ok_or_else(|| bad("snappy literal end overflow"))?;
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
                    _ /* copy, 4-byte offset (tag type 3) */ => {
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
        // truncated (empty): varint() on empty src returns Err → Bad
        assert!(matches!(decompress(&[], 0), Err(PqError::Bad(_))));
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
