//! SP-PG-COPY — COPY protocol message encoders + decoders.
//!
//! PG §55.7 wire shapes for the five COPY-mode message tags V1 cares
//! about:
//!
//! | Tag | Direction | Wire shape |
//! |---|---|---|
//! | `G` CopyInResponse  | server→client | `G [length:4] [format:u8] [ncols:i16] [code:i16]×ncols` |
//! | `H` CopyOutResponse | server→client | `H [length:4] [format:u8] [ncols:i16] [code:i16]×ncols` |
//! | `d` CopyData        | both          | `d [length:4] [data:bytes]` |
//! | `c` CopyDone        | both          | `c [length:4 = 4]` |
//! | `f` CopyFail        | client→server | `f [length:4] [reason\0]` |
//!
//! V1 emits `format=0` (text) for every `G`/`H` and per-column code=0.
//! Binary format is V2 (SP-PG-COPY-BIN).
//!
//! All encoders + decoders are byte-locked against PG §55.7 canonical
//! shape via T1 KATs — a 1-byte drift here would silently break every
//! PG client (pg_dump, sysbench, psql `\copy` all hard-code these
//! shapes).

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::proto::{
    BE_COMMAND_COMPLETE, FE_COPY_DATA, FE_COPY_DONE, FORMAT_CODE_TEXT,
};

/// `G` CopyInResponse tag. Server sends in reply to a `Q` containing
/// `COPY <table> FROM STDIN`. Same byte value as the kernel-reserved
/// SIGTERM signal — coincidence; PG picked it because `G` stands for
/// "get".
pub const BE_COPY_IN_RESPONSE: u8 = b'G';

/// `H` CopyOutResponse tag. Server sends in reply to a `Q` containing
/// `COPY <table> TO STDOUT`. `H` for "have data" historically.
pub const BE_COPY_OUT_RESPONSE: u8 = b'H';

/// Server-side `c` CopyDone tag. Same byte as the frontend `c`
/// CopyDone — the protocol disambiguates by direction.
pub const BE_COPY_DONE: u8 = FE_COPY_DONE;

/// Server-side `d` CopyData tag. Same byte as the frontend `d`
/// CopyData. The two directions share the wire shape; the meaning
/// changes (client→server is row-input bytes, server→client is row-
/// output bytes).
pub const BE_COPY_DATA: u8 = FE_COPY_DATA;

/// Errors `decode_copy_fail` can return. The payload is supposed to
/// be a NUL-terminated reason cstring; a malformed payload is treated
/// as a protocol violation by the caller (the connection STAYS ALIVE
/// per spec §6 tolerant contract, with 08P01 + RFQ).
#[derive(Debug, PartialEq, Eq)]
pub enum CopyFailParseError {
    MissingNulTerminator,
    EmbeddedNul,
    NotUtf8,
    EmptyBody,
}

/// Encodes a `G` CopyInResponse. `ncols` is the number of columns
/// in the target table — every per-column format code is text (0) in
/// V1.
///
/// Wire: `G [length:4 BE] [format:u8=0] [ncols:i16 BE] [code:i16
/// BE]×ncols`. The length includes itself but NOT the tag byte:
/// `length = 4 (self) + 1 (format) + 2 (ncols) + 2*ncols`.
pub fn encode_copy_in_response(ncols: u16) -> Vec<u8> {
    encode_copy_n_response(BE_COPY_IN_RESPONSE, ncols)
}

/// Encodes a `H` CopyOutResponse. Same wire shape as
/// `CopyInResponse`; differs only by the tag byte. V1 emits text
/// format for every column.
pub fn encode_copy_out_response(ncols: u16) -> Vec<u8> {
    encode_copy_n_response(BE_COPY_OUT_RESPONSE, ncols)
}

/// Shared helper — `G` and `H` have the SAME wire shape modulo the
/// tag byte. PG §55.7 explicitly notes the symmetry; sharing the
/// encoder locks the byte-equality invariant across the two directions.
fn encode_copy_n_response(tag: u8, ncols: u16) -> Vec<u8> {
    let n = ncols as usize;
    // length = 4 (self) + 1 (format) + 2 (ncols) + 2*n (per-col codes).
    let length = (4 + 1 + 2 + 2 * n) as u32;
    let mut frame = Vec::with_capacity(1 + length as usize);
    frame.push(tag);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.push(FORMAT_CODE_TEXT as u8); // overall format = 0 (text)
    frame.extend_from_slice(&ncols.to_be_bytes());
    for _ in 0..n {
        frame.extend_from_slice(&FORMAT_CODE_TEXT.to_be_bytes());
    }
    frame
}

/// Encodes a `d` CopyData frame. `data` is the raw payload bytes
/// the server sends to the client (one row of text-format bytes
/// including its trailing `\n`, in V1's COPY TO path).
///
/// Wire: `d [length:4 BE] [data:bytes]`. `length = 4 + data.len()`.
pub fn encode_copy_data(data: &[u8]) -> Vec<u8> {
    let length = (4 + data.len()) as u32;
    let mut frame = Vec::with_capacity(1 + length as usize);
    frame.push(BE_COPY_DATA);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(data);
    frame
}

/// Encodes a `c` CopyDone frame (server→client). 5-byte
/// `c [length:4 BE = 4]` envelope — no payload.
pub fn encode_copy_done() -> Vec<u8> {
    vec![BE_COPY_DONE, 0, 0, 0, 4]
}

/// Decodes the BODY of a `f` CopyFail frame (i.e. the bytes AFTER
/// `server::read_message` has stripped the 1-byte type tag and 4-byte
/// length prefix). Returns the client-supplied reason string (with
/// the trailing NUL stripped).
///
/// PG §55.7 — the payload is `[reason:cstring]`. A client aborting
/// a COPY FROM STDIN exchange (e.g. psql `\.` end-of-data with no
/// preceding `\copy`, or an application-level error mid-stream)
/// sends this frame; the server is supposed to surface the reason
/// as part of the canonical 57014 ErrorResponse it emits.
pub fn decode_copy_fail(body: &[u8]) -> Result<&str, CopyFailParseError> {
    if body.is_empty() {
        return Err(CopyFailParseError::EmptyBody);
    }
    let last = *body.last().expect("non-empty checked above");
    if last != 0 {
        return Err(CopyFailParseError::MissingNulTerminator);
    }
    let bytes = &body[..body.len() - 1];
    if bytes.iter().any(|&b| b == 0) {
        return Err(CopyFailParseError::EmbeddedNul);
    }
    std::str::from_utf8(bytes).map_err(|_| CopyFailParseError::NotUtf8)
}

/// Builds the canonical PG `COPY N` CommandComplete tag string for
/// `encode_command_complete`. PG §SQL-COPY: "On successful completion,
/// a COPY command returns a command tag of the form `COPY count`."
pub fn copy_tag(rows: u64) -> String {
    format!("COPY {rows}")
}

/// Re-export the `encode_command_complete` glue at the COPY surface
/// so callers can compose `encode_copy_done()` + `encode_command_
/// complete(&copy_tag(N))` + `encode_ready_for_query` for the COPY
/// TO finalize sequence without crossing module boundaries.
pub use crate::response::encode_command_complete;

/// Re-export the BE_COMMAND_COMPLETE tag for byte-locked KAT
/// assertions in the parent module.
pub const BE_COPY_COMMAND_COMPLETE: u8 = BE_COMMAND_COMPLETE;

#[cfg(test)]
mod tests {
    use super::*;

    // ───────────────────────────────────────────────────────────────────
    // T1 KATs — every encoder byte-locked vs PG §55.7 canonical shape.
    // A 1-byte drift here would silently break every PG client; the
    // KATs catch that.
    // ───────────────────────────────────────────────────────────────────

    /// `G` CopyInResponse with 0 columns (degenerate but valid wire
    /// shape — PG would emit this for a `COPY t FROM STDIN` on a
    /// zero-column table, which doesn't exist in practice but the
    /// codec stays honest).
    #[test]
    fn t1_copy_in_response_zero_cols_byte_locked() {
        let frame = encode_copy_in_response(0);
        // 'G' + length=7 + format=0 + ncols=0 (no per-col codes)
        let expected = vec![b'G', 0, 0, 0, 7, 0, 0, 0];
        assert_eq!(frame, expected);
        assert_eq!(frame.len(), 8);
    }

    /// `G` CopyInResponse with 2 columns — the bread-and-butter shape
    /// every pg_dump/`\copy` exchange exercises.
    #[test]
    fn t1_copy_in_response_two_cols_byte_locked() {
        let frame = encode_copy_in_response(2);
        // 'G' + length=11 + format=0 + ncols=2 + code=0 + code=0
        // length = 4 (self) + 1 + 2 + 2*2 = 11
        let mut expected = Vec::new();
        expected.push(b'G');
        expected.extend_from_slice(&11u32.to_be_bytes());
        expected.push(0); // format = text
        expected.extend_from_slice(&2u16.to_be_bytes()); // ncols
        expected.extend_from_slice(&0u16.to_be_bytes()); // col 0 format
        expected.extend_from_slice(&0u16.to_be_bytes()); // col 1 format
        assert_eq!(frame, expected);
        assert_eq!(frame.len(), 12);
    }

    /// `H` CopyOutResponse shape — identical to `G` modulo the tag
    /// byte. Locked so a future refactor cannot accidentally invert
    /// the two encoders.
    #[test]
    fn t1_copy_out_response_two_cols_byte_locked() {
        let frame = encode_copy_out_response(2);
        let mut expected = Vec::new();
        expected.push(b'H');
        expected.extend_from_slice(&11u32.to_be_bytes());
        expected.push(0);
        expected.extend_from_slice(&2u16.to_be_bytes());
        expected.extend_from_slice(&0u16.to_be_bytes());
        expected.extend_from_slice(&0u16.to_be_bytes());
        assert_eq!(frame, expected);
        assert_eq!(frame.len(), 12);
    }

    /// G and H differ ONLY by the tag byte — locking the symmetry
    /// invariant per PG §55.7. A refactor that drifts e.g. the
    /// format byte position of one but not the other would silently
    /// corrupt one direction.
    #[test]
    fn t1_copy_in_and_out_responses_differ_only_by_tag_byte() {
        for n in [0u16, 1, 2, 7, 100] {
            let g = encode_copy_in_response(n);
            let h = encode_copy_out_response(n);
            assert_eq!(g.len(), h.len(), "ncols={n}: same total length");
            // Tags differ.
            assert_eq!(g[0], b'G');
            assert_eq!(h[0], b'H');
            // Everything after the tag is byte-equal.
            assert_eq!(&g[1..], &h[1..], "ncols={n}: bytes after tag must match");
        }
    }

    /// `d` CopyData frame with a typical row payload — `\n`-terminated
    /// tab-separated text. The wire bytes carry the entire payload
    /// verbatim (the framing layer doesn't transform).
    #[test]
    fn t1_copy_data_emits_typed_frame() {
        let payload = b"1\tworld\n";
        let frame = encode_copy_data(payload);
        // 'd' + length=4+8 + payload
        let mut expected = vec![b'd'];
        expected.extend_from_slice(&12u32.to_be_bytes());
        expected.extend_from_slice(payload);
        assert_eq!(frame, expected);
        assert_eq!(frame.len(), 13);
    }

    /// `d` CopyData with empty payload — length=4 (just the length
    /// field, no data). A client COULD send this; the codec stays
    /// honest.
    #[test]
    fn t1_copy_data_empty_payload_is_just_envelope() {
        let frame = encode_copy_data(b"");
        assert_eq!(frame, vec![b'd', 0, 0, 0, 4]);
        assert_eq!(frame.len(), 5);
    }

    /// `c` CopyDone — 5-byte envelope, no payload.
    #[test]
    fn t1_copy_done_is_five_bytes() {
        let frame = encode_copy_done();
        assert_eq!(frame, vec![b'c', 0, 0, 0, 4]);
        assert_eq!(frame.len(), 5);
    }

    /// `f` CopyFail decoder — happy path returns the reason string
    /// with trailing NUL stripped.
    #[test]
    fn t1_copy_fail_decoder_happy_path() {
        let body = b"client aborted: out of disk\0";
        assert_eq!(
            decode_copy_fail(body),
            Ok("client aborted: out of disk")
        );
    }

    /// `f` CopyFail decoder — empty body → EmptyBody error.
    #[test]
    fn t1_copy_fail_decoder_empty_body() {
        assert_eq!(decode_copy_fail(b""), Err(CopyFailParseError::EmptyBody));
    }

    /// `f` CopyFail decoder — missing trailing NUL → error.
    #[test]
    fn t1_copy_fail_decoder_missing_nul() {
        assert_eq!(
            decode_copy_fail(b"reason without nul"),
            Err(CopyFailParseError::MissingNulTerminator)
        );
    }

    /// `f` CopyFail decoder — embedded NUL → error (rather than
    /// truncating the reason silently).
    #[test]
    fn t1_copy_fail_decoder_embedded_nul() {
        assert_eq!(
            decode_copy_fail(b"part one\0part two\0"),
            Err(CopyFailParseError::EmbeddedNul)
        );
    }

    /// `f` CopyFail decoder — invalid UTF-8 → error.
    #[test]
    fn t1_copy_fail_decoder_invalid_utf8() {
        assert_eq!(
            decode_copy_fail(&[0xC3, 0x28, 0]),
            Err(CopyFailParseError::NotUtf8)
        );
    }

    /// `f` CopyFail decoder — body of just `\0` (zero-length reason)
    /// parses to empty string. Locked because empty-reason is the
    /// "no detail" case psql emits for `\copy` aborts.
    #[test]
    fn t1_copy_fail_decoder_zero_length_reason() {
        assert_eq!(decode_copy_fail(b"\0"), Ok(""));
    }

    /// `copy_tag` builds the canonical PG `COPY N` CommandComplete
    /// tag string. PG §SQL-COPY locks this shape — psql, JDBC,
    /// asyncpg, pg_dump all parse the `COPY` prefix + a decimal
    /// integer.
    #[test]
    fn t1_copy_tag_canonical_shape() {
        assert_eq!(copy_tag(0), "COPY 0");
        assert_eq!(copy_tag(1), "COPY 1");
        assert_eq!(copy_tag(1000), "COPY 1000");
        // Very large counts (u64::MAX) — render as decimal, no
        // overflow.
        assert_eq!(copy_tag(u64::MAX), "COPY 18446744073709551615");
    }

    /// Tag-byte distinctness across the COPY-mode encoders. A byte-
    /// flip refactor that confused `G` with `H` would silently
    /// invert the direction every client sees.
    #[test]
    fn t1_copy_tag_bytes_are_distinct() {
        let tags: Vec<u8> = vec![
            encode_copy_in_response(0)[0],
            encode_copy_out_response(0)[0],
            encode_copy_data(b"")[0],
            encode_copy_done()[0],
        ];
        let unique: std::collections::HashSet<u8> = tags.iter().copied().collect();
        assert_eq!(unique.len(), tags.len(), "COPY tag bytes must be distinct");
        assert_eq!(tags, vec![b'G', b'H', b'd', b'c']);
    }
}
