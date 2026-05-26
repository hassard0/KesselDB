//! LZ4 block decompression — RAW format (no Hadoop-style framing).
//! Used by Parquet's LZ4_RAW codec (the modern default since pyarrow v8).
//! Pyarrow's legacy LZ4 (deprecated, with 8-byte big-endian framing) is
//! NOT supported in V1 — rejected at the Codec::Lz4 dispatch site with a
//! named Unsupported error pointing at this slice.
//!
//! LZ4 block format reference:
//!   https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md
//!
//! Each LZ4 sequence is:
//!   [token byte: high nibble = literal_length, low nibble = match_length]
//!   [literal_length_extra bytes if literal_length == 15 (sums until <255)]
//!   [literal_length bytes of raw literals]
//!   [2-byte LE offset]
//!   [match_length_extra bytes if match_length == 15 (sums until <255)]
//!
//! Actual `match_length` = encoded value + 4 (minmatch=4). End-of-block:
//! the final sequence has no offset+match_length — just literals to end.

#![allow(dead_code)]

use crate::PqError;

pub fn decompress(src: &[u8], expected_uncompressed_size: usize) -> Result<Vec<u8>, PqError> {
    let mut out: Vec<u8> = Vec::with_capacity(expected_uncompressed_size.min(64 * 1024));
    let mut i: usize = 0;

    while i < src.len() {
        // Read token.
        let token = *src.get(i).ok_or_else(|| PqError::Bad("lz4: truncated token".into()))?;
        i += 1;

        let mut lit_len = (token >> 4) as usize;
        if lit_len == 15 {
            loop {
                let b = *src.get(i).ok_or_else(|| PqError::Bad("lz4: truncated lit-len extra".into()))?;
                i += 1;
                lit_len = lit_len.checked_add(b as usize).ok_or_else(|| PqError::Bad("lz4: lit-len overflow".into()))?;
                if b != 255 { break; }
            }
        }

        // Literals.
        let lit_end = i.checked_add(lit_len).ok_or_else(|| PqError::Bad("lz4: lit overrun overflow".into()))?;
        if lit_end > src.len() {
            return Err(PqError::Bad(format!(
                "lz4: literal section overruns src (need {lit_end} <= {})", src.len()
            )));
        }
        if out.len().saturating_add(lit_len) > expected_uncompressed_size {
            return Err(PqError::Bad("lz4: literals exceed declared uncompressed size".into()));
        }
        out.extend_from_slice(src.get(i..lit_end).unwrap_or(&[]));
        i = lit_end;

        // End of block — no match section if we've consumed all of src.
        if i == src.len() {
            break;
        }

        // Offset (2-byte LE).
        if i + 2 > src.len() {
            return Err(PqError::Bad("lz4: truncated offset".into()));
        }
        let offset = u16::from_le_bytes([src[i], src[i + 1]]) as usize;
        i += 2;
        if offset == 0 {
            return Err(PqError::Bad("lz4: zero offset (invalid per spec)".into()));
        }
        if offset > out.len() {
            return Err(PqError::Bad(format!(
                "lz4: offset {offset} > out length {}", out.len()
            )));
        }

        // Match length.
        let mut match_len = (token & 0x0f) as usize + 4;  // minmatch=4
        if (token & 0x0f) == 15 {
            loop {
                let b = *src.get(i).ok_or_else(|| PqError::Bad("lz4: truncated match-len extra".into()))?;
                i += 1;
                match_len = match_len.checked_add(b as usize).ok_or_else(|| PqError::Bad("lz4: match-len overflow".into()))?;
                if b != 255 { break; }
            }
        }

        if out.len().saturating_add(match_len) > expected_uncompressed_size {
            return Err(PqError::Bad("lz4: match exceeds declared uncompressed size".into()));
        }

        // Copy from `offset` bytes back in `out`. LZ4 allows overlapping
        // copies (offset < match_len), which is the LZ77 RLE trick — copy
        // byte-by-byte forward, NOT bulk memcpy, because the source-region
        // grows as we write.
        let copy_start = out.len() - offset;
        for j in 0..match_len {
            let b = out[copy_start + j];
            out.push(b);
        }
    }

    if out.len() != expected_uncompressed_size {
        return Err(PqError::Bad(format!(
            "lz4: decoded {} bytes, expected {}", out.len(), expected_uncompressed_size
        )));
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kat_literal_only_block() {
        // Block: literal "hello" (5 bytes), no match.
        // Token: lit_len=5 -> 0x50. Literals: [0x68, 0x65, 0x6c, 0x6c, 0x6f].
        // No more bytes (end-of-block — i == src.len() right after literals).
        let src = vec![0x50, 0x68, 0x65, 0x6c, 0x6c, 0x6f];
        let out = decompress(&src, 5).expect("decompress");
        assert_eq!(out, b"hello");
    }

    #[test]
    fn kat_lit_then_match() {
        // Sequence 1: token=0x40, literals "abcd", offset=4, match_len-4=0
        //   -> token 0x40, literals [0x61,0x62,0x63,0x64], offset [0x04,0x00]
        //   = decodes to "abcd" + match "abcd" = "abcdabcd"
        // Sequence 2 (end-of-block, literal-only): token=0x50, literals "wxyz!"
        //   -> token 0x50, literals [0x77,0x78,0x79,0x7a,0x21]
        // Total: "abcdabcdwxyz!" = 13 bytes.
        let src = vec![
            0x40, 0x61, 0x62, 0x63, 0x64, 0x04, 0x00,  // seq 1
            0x50, 0x77, 0x78, 0x79, 0x7a, 0x21,        // seq 2 (eob)
        ];
        let out = decompress(&src, 13).expect("decompress");
        assert_eq!(out, b"abcdabcdwxyz!");
    }

    #[test]
    fn kat_long_literal_extra_bytes() {
        // Test the lit_len == 15 extra-byte path. 20 literals.
        // lit_len=20 -> token high nibble = 15 with one extra byte = 5
        //   (extra-byte loop terminates since 5 < 255).
        // Token: 0xf0 (high=15, low=0 — no match section since src ends).
        let literals: Vec<u8> = b"0123456789abcdefghij".to_vec();
        let mut src = vec![0xf0, 0x05];
        src.extend_from_slice(&literals);
        let out = decompress(&src, 20).expect("decompress");
        assert_eq!(out, literals);
    }

    #[test]
    fn kat_rejects_zero_offset() {
        // token 0x40 (lit_len=4, match_len=4+0=4)
        // literals "abcd" (4 bytes)
        // offset 0 (invalid per LZ4 spec)
        let src = vec![0x40, 0x61, 0x62, 0x63, 0x64, 0x00, 0x00];
        let err = decompress(&src, 8).unwrap_err();
        assert!(format!("{err:?}").contains("zero offset"), "got: {err:?}");
    }

    #[test]
    fn kat_rejects_size_mismatch() {
        // Decode 5 literals but declare expected_size=10 -> mismatch
        let src = vec![0x50, 0x68, 0x65, 0x6c, 0x6c, 0x6f];
        let err = decompress(&src, 10).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("decoded 5") || msg.contains("expected 10"), "got: {msg}");
    }

    #[test]
    fn kat_overlapping_copy_rle() {
        // The LZ77 RLE trick — offset=1, match_len > 1 repeats a single byte.
        // Sequence: literal "a" (1 byte), offset=1, match_len=4+0=4
        //   -> output "a" + 4 more "a"s = "aaaaa" (5 bytes)
        // Then EOB literal "z" (1 byte) so the EOB literals tail exists.
        // token1 0x10 (lit_len=1, match_len-4=0), literals [0x61], offset [0x01,0x00]
        // token2 0x10 (lit_len=1, match_len ignored — but low nibble=0 means EOB needed)
        //   Actually token2 must be literal-only EOB. Use 0x10 only if followed by NO offset bytes.
        //   src.len after literals must equal i for EOB.
        // Compact: just RLE then literal EOB.
        let src = vec![
            0x10, 0x61, 0x01, 0x00,  // seq 1: "a" then RLE match 4x at offset 1 -> "aaaaa"
            0x10, 0x7a,              // seq 2 (eob): literal "z"
        ];
        let out = decompress(&src, 6).expect("rle decompress");
        assert_eq!(out, b"aaaaaz");
    }
}
