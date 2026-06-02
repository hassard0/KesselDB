//! SP-PG-COPY-BIN — PostgreSQL `COPY ... WITH (FORMAT binary)` codec
//! per PG §55.2.7.
//!
//! **T1+T2 status (this commit):** binary wire-format codec —
//!
//! - **Header**: 11-byte signature `PGCOPY\n\xff\r\n\0` + 4-byte BE
//!   flags + 4-byte BE header extension length + extension bytes.
//! - **Row**: 2-byte BE i16 field count, then per field: 4-byte BE i32
//!   length (`-1` = NULL), then `length` bytes of binary-encoded value.
//! - **End-of-data marker**: 2-byte BE i16 `-1` (`0xff 0xff`).
//!
//! Per-column binary encoding is the SAME as the Bind/Execute binary
//! parameter path (SP-PG-EXTQ-BIN) and the binary RowDescription/DataRow
//! path (SP-PG-EXTQ-BIN-RESULTS) — V1 REUSES `extq::binary_results::
//! encode_binary_value` and `extq::substitute::decode_binary_param`
//! verbatim. Only the framing layer lives here.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-06-02-kesseldb-sppgcopybin-design.md`
//!
//! ## Locked invariants (T1+T2)
//!
//! - `PG_BINARY_SIGNATURE` byte-equal to `b"PGCOPY\n\xff\r\n\0"` (the
//!   exact 11-byte sequence PG §55.2.7 specifies — drift here would
//!   silently break every pg_dump custom-format restore).
//! - `encode_binary_header()` returns 19 bytes: signature + 4 zero
//!   flag bytes + 4 zero extension-length bytes.
//! - `encode_binary_end_of_data()` returns 2 bytes `0xff 0xff`.
//! - `encode_binary_row(values)` matches PG §55.2.7: 2-byte BE
//!   field count + per-field 4-byte BE length + binary bytes.
//! - `BinaryDecoder::consume_header` validates signature; rejects bad
//!   signature with `BadSignature`; rejects non-zero flags with
//!   `UnsupportedFlags`; rejects oversized extension with
//!   `HeaderExtensionTooLarge`.
//! - `BinaryDecoder::next_row` round-trips with `encode_binary_row` for
//!   every shape — single column, multi column, NULL columns, empty rows.

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::copy::MAX_COPY_DATA_BUFFER;

/// PG §55.2.7 — the 11-byte canonical signature every binary COPY
/// stream starts with. The trailing carriage-return-line-feed-NUL is
/// designed to detect end-of-line conversion (DOS↔Unix line endings),
/// the `\xff` is designed to detect 8-bit-clean transport (a 7-bit
/// channel would strip the high bit), and `PGCOPY` is the human-readable
/// magic.
pub const PG_BINARY_SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";

/// PG §55.2.7 — the 2-byte BE i16 end-of-data marker. `-1` cast to u16
/// = `0xffff`. Per PG v3 protocol the marker is redundant (CopyDone
/// is the authoritative end-of-stream signal), but PG itself still
/// emits it and accepts it from clients. V1 tolerates on input and
/// emits on output.
pub const PG_BINARY_END_OF_DATA: i16 = -1;

/// Streaming binary COPY decoder. Holds an immutable byte slice + a
/// cursor + a state flag. Caller constructs over `state.carry + data`
/// each CopyData frame, consumes whatever rows it can, and reports
/// back the new cursor so the caller updates the carry buffer to
/// `bytes[cursor..]`.
#[derive(Debug)]
pub struct BinaryDecoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
    state: BinaryState,
}

/// Decoder lifecycle. The header MUST be consumed before any row is
/// parsed. `EndOfData` is terminal — subsequent `next_row` calls return
/// `Ok(None)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryState {
    /// Header not yet consumed — call `consume_header` first.
    Header,
    /// Header consumed — `next_row` returns rows until end-of-data.
    Body,
    /// End-of-data marker seen — no more rows.
    EndOfData,
}

/// Errors `BinaryDecoder` can return. Each maps at the caller to a PG
/// SQLSTATE per spec §6.
#[derive(Debug, PartialEq, Eq)]
pub enum BinaryDecodeError {
    /// Signature mismatch (not the canonical `PGCOPY\n\xff\r\n\0`).
    /// Maps to SQLSTATE `08P01 protocol_violation`.
    BadSignature,
    /// Header flag bits other than 0 set. V1 only supports flags=0;
    /// bit 16 (OID column) is V2 SP-PG-COPY-BIN-OID. Maps to SQLSTATE
    /// `0A000 feature_not_supported`.
    UnsupportedFlags { flags: u32 },
    /// Header extension area > MAX_COPY_DATA_BUFFER (16 MiB). Maps to
    /// SQLSTATE `08P01`.
    HeaderExtensionTooLarge { length: u32 },
    /// A row's field count differs from the expected column count.
    /// Maps to SQLSTATE `22023 invalid_parameter_value`.
    FieldCountMismatch { expected: usize, actual: usize },
    /// A field's length prefix is negative (other than -1 which means
    /// NULL). Maps to SQLSTATE `08P01`.
    BadFieldLength { length: i32 },
    /// Truncated input — ran out of bytes mid-header / mid-row. The
    /// caller distinguishes "need more data" (`next_row -> Ok(None)`)
    /// from "fatal truncation" (`Err(Truncated)`) by where the call
    /// came from: streaming callers carry partial bytes and retry;
    /// finalize callers (CopyDone) treat partial as fatal.
    Truncated,
}

impl<'a> BinaryDecoder<'a> {
    /// Build a fresh decoder over `bytes` starting at offset 0 in the
    /// `Header` state.
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            cursor: 0,
            state: BinaryState::Header,
        }
    }

    /// Build a decoder already past the header — used when the caller
    /// stashed the binary_header_consumed flag in `CopyInState` and is
    /// resuming over a fresh CopyData frame's bytes.
    pub fn new_in_body(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            cursor: 0,
            state: BinaryState::Body,
        }
    }

    /// Current decoder state.
    pub fn state(&self) -> BinaryState {
        self.state
    }

    /// Current cursor offset within `bytes`. The caller updates the
    /// carry buffer to `bytes[cursor..]` on `Ok(None)` returns.
    pub fn cursor(&self) -> usize {
        self.cursor
    }

    /// Try to consume the 19-byte (or longer if header extension > 0)
    /// header. Returns:
    ///
    /// - `Ok(true)` — header successfully consumed; state is now `Body`.
    /// - `Ok(false)` — not enough bytes yet; state stays `Header`; the
    ///   caller carries `bytes[cursor..]` (which is `bytes` in this
    ///   case since we made no progress) and retries on the next frame.
    /// - `Err(...)` — malformed signature / unsupported flags / oversized
    ///   extension.
    pub fn consume_header(&mut self) -> Result<bool, BinaryDecodeError> {
        debug_assert_eq!(self.state, BinaryState::Header);
        // Minimum header: 11 (sig) + 4 (flags) + 4 (extension length).
        let min = 11 + 4 + 4;
        if self.bytes.len() < min {
            return Ok(false);
        }
        // Signature check.
        if &self.bytes[..11] != PG_BINARY_SIGNATURE {
            return Err(BinaryDecodeError::BadSignature);
        }
        // Flags (BE u32). V1: only 0 is supported.
        let flags = u32::from_be_bytes([
            self.bytes[11],
            self.bytes[12],
            self.bytes[13],
            self.bytes[14],
        ]);
        if flags != 0 {
            return Err(BinaryDecodeError::UnsupportedFlags { flags });
        }
        // Extension length (BE u32).
        let ext_len = u32::from_be_bytes([
            self.bytes[15],
            self.bytes[16],
            self.bytes[17],
            self.bytes[18],
        ]);
        if (ext_len as usize) > MAX_COPY_DATA_BUFFER {
            return Err(BinaryDecodeError::HeaderExtensionTooLarge {
                length: ext_len,
            });
        }
        let needed = 19 + ext_len as usize;
        if self.bytes.len() < needed {
            // Header advertises an extension but we don't have all the
            // bytes yet. Stay in Header state; caller retries.
            return Ok(false);
        }
        self.cursor = needed;
        self.state = BinaryState::Body;
        Ok(true)
    }

    /// Parse the next row. Returns:
    ///
    /// - `Ok(Some(fields))` — a complete row was parsed; `fields[i] ==
    ///   None` for NULL columns, `Some(&bytes)` for non-NULL.
    /// - `Ok(None)` — either (a) more bytes are needed (state stays
    ///   `Body`; caller carries `bytes[cursor..]`) OR (b) the
    ///   end-of-data marker was consumed (state is now `EndOfData`;
    ///   caller distinguishes via `state()`).
    /// - `Err(...)` — malformed row.
    pub fn next_row(
        &mut self,
        expected_cols: usize,
    ) -> Result<Option<Vec<Option<&'a [u8]>>>, BinaryDecodeError> {
        debug_assert_ne!(self.state, BinaryState::Header);
        if self.state == BinaryState::EndOfData {
            return Ok(None);
        }
        let start = self.cursor;
        // Need at least 2 bytes for the field count.
        if start + 2 > self.bytes.len() {
            return Ok(None);
        }
        let field_count =
            i16::from_be_bytes([self.bytes[start], self.bytes[start + 1]]);
        if field_count == PG_BINARY_END_OF_DATA {
            self.cursor = start + 2;
            self.state = BinaryState::EndOfData;
            return Ok(None);
        }
        if field_count < 0 {
            return Err(BinaryDecodeError::BadFieldLength {
                length: field_count as i32,
            });
        }
        let actual = field_count as usize;
        if actual != expected_cols {
            return Err(BinaryDecodeError::FieldCountMismatch {
                expected: expected_cols,
                actual,
            });
        }
        let mut cursor = start + 2;
        let mut fields: Vec<Option<&'a [u8]>> = Vec::with_capacity(actual);
        for _ in 0..actual {
            if cursor + 4 > self.bytes.len() {
                // Partial — caller carries.
                return Ok(None);
            }
            let len = i32::from_be_bytes([
                self.bytes[cursor],
                self.bytes[cursor + 1],
                self.bytes[cursor + 2],
                self.bytes[cursor + 3],
            ]);
            cursor += 4;
            if len == -1 {
                fields.push(None);
                continue;
            }
            if len < 0 {
                return Err(BinaryDecodeError::BadFieldLength { length: len });
            }
            let n = len as usize;
            if cursor + n > self.bytes.len() {
                // Partial — caller carries.
                return Ok(None);
            }
            fields.push(Some(&self.bytes[cursor..cursor + n]));
            cursor += n;
        }
        self.cursor = cursor;
        Ok(Some(fields))
    }
}

/// Encode the canonical 19-byte PG binary COPY header (signature + 0
/// flags + 0-length extension area).
///
/// V1 always emits flags=0 (no OID column — V2 SP-PG-COPY-BIN-OID
/// lifts) and a zero-length extension area.
pub fn encode_binary_header() -> Vec<u8> {
    let mut out = Vec::with_capacity(19);
    out.extend_from_slice(PG_BINARY_SIGNATURE);
    out.extend_from_slice(&0u32.to_be_bytes()); // flags
    out.extend_from_slice(&0u32.to_be_bytes()); // header extension length
    out
}

/// Encode the 2-byte i16 -1 end-of-data marker. PG v3 protocol treats
/// this as advisory (CopyDone is the authoritative signal), but PG
/// itself still emits + accepts so V1 mirrors.
pub fn encode_binary_end_of_data() -> Vec<u8> {
    PG_BINARY_END_OF_DATA.to_be_bytes().to_vec()
}

/// Encode a single binary COPY row.
///
/// Wire: `[field_count:i16 BE][len:i32 BE | value]×N`. `-1` length =
/// NULL (no value bytes). `Some(&[u8])` = the binary value bytes (the
/// caller already ran `extq::binary_results::encode_binary_value` to
/// produce these bytes per column type).
pub fn encode_binary_row(values: &[Option<&[u8]>]) -> Vec<u8> {
    // Pre-compute capacity to avoid re-allocs on common row sizes.
    let cap = 2 + values
        .iter()
        .map(|v| 4 + v.map(|b| b.len()).unwrap_or(0))
        .sum::<usize>();
    let mut out = Vec::with_capacity(cap);
    let field_count = values.len() as i16;
    out.extend_from_slice(&field_count.to_be_bytes());
    for v in values {
        match v {
            None => out.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(bytes) => {
                let len = bytes.len() as i32;
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(bytes);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── encode helpers — byte-locked vs PG §55.2.7 ────────────────────

    /// SP-PG-COPY-BIN T1: signature constant is byte-equal to the PG-
    /// canonical 11-byte sequence. A drift here silently breaks every
    /// pg_dump --format=custom restore.
    #[test]
    fn t1_signature_constant_byte_locked() {
        assert_eq!(PG_BINARY_SIGNATURE.len(), 11);
        assert_eq!(
            &PG_BINARY_SIGNATURE[..],
            &[b'P', b'G', b'C', b'O', b'P', b'Y', b'\n', 0xff, b'\r', b'\n', 0x00]
        );
    }

    /// SP-PG-COPY-BIN T1: `encode_binary_header` returns 19 bytes
    /// = signature(11) + flags(4) + extension_len(4), all flag +
    /// extension bytes zero.
    #[test]
    fn t1_encode_binary_header_byte_locked() {
        let h = encode_binary_header();
        let mut expected: Vec<u8> = Vec::new();
        expected.extend_from_slice(PG_BINARY_SIGNATURE);
        expected.extend_from_slice(&[0, 0, 0, 0]); // flags = 0
        expected.extend_from_slice(&[0, 0, 0, 0]); // ext len = 0
        assert_eq!(h, expected);
        assert_eq!(h.len(), 19);
    }

    /// SP-PG-COPY-BIN T1: `encode_binary_end_of_data` returns 2 bytes
    /// `0xff 0xff`.
    #[test]
    fn t1_encode_binary_end_of_data_byte_locked() {
        assert_eq!(encode_binary_end_of_data(), vec![0xff, 0xff]);
    }

    /// SP-PG-COPY-BIN T1: a single-column INT8=42 row encodes to
    /// `\x00\x01\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x2a`.
    /// 2 bytes field_count + 4 bytes length + 8 bytes BE i64.
    #[test]
    fn t1_encode_binary_row_single_int8_byte_locked() {
        let int8_bytes: [u8; 8] = 42i64.to_be_bytes();
        let row = encode_binary_row(&[Some(&int8_bytes)]);
        assert_eq!(
            row,
            vec![
                0x00, 0x01, // field count = 1
                0x00, 0x00, 0x00, 0x08, // length = 8
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, // value
            ]
        );
    }

    /// SP-PG-COPY-BIN T1: a multi-column row (INT8=42 + TEXT="hi")
    /// emits the right field count and per-column lengths.
    #[test]
    fn t1_encode_binary_row_int8_and_text() {
        let int8_bytes: [u8; 8] = 42i64.to_be_bytes();
        let text_bytes: [u8; 2] = *b"hi";
        let row = encode_binary_row(&[Some(&int8_bytes), Some(&text_bytes)]);
        let mut expected: Vec<u8> = Vec::new();
        expected.extend_from_slice(&2i16.to_be_bytes()); // field count = 2
        expected.extend_from_slice(&8i32.to_be_bytes()); // col 0 len = 8
        expected.extend_from_slice(&int8_bytes);
        expected.extend_from_slice(&2i32.to_be_bytes()); // col 1 len = 2
        expected.extend_from_slice(&text_bytes);
        assert_eq!(row, expected);
    }

    /// SP-PG-COPY-BIN T1: a NULL column encodes as -1 length + no
    /// value bytes.
    #[test]
    fn t1_encode_binary_row_null_column_byte_locked() {
        let row = encode_binary_row(&[None]);
        // field count = 1, then i32 -1 for the length, no value.
        let mut expected: Vec<u8> = Vec::new();
        expected.extend_from_slice(&1i16.to_be_bytes());
        expected.extend_from_slice(&(-1i32).to_be_bytes());
        assert_eq!(row, expected);
        assert_eq!(row.len(), 2 + 4);
    }

    /// SP-PG-COPY-BIN T1: encode + decode round-trip identity for a
    /// 2-column row with one non-NULL and one NULL.
    #[test]
    fn t1_encode_decode_round_trip_mixed_null() {
        let value_bytes: [u8; 4] = 100i32.to_be_bytes();
        let row = encode_binary_row(&[Some(&value_bytes), None]);
        // Decode by chaining header+row into a single stream the
        // decoder can chew.
        let mut stream = encode_binary_header();
        stream.extend_from_slice(&row);
        stream.extend_from_slice(&encode_binary_end_of_data());
        let mut dec = BinaryDecoder::new(&stream);
        assert!(dec.consume_header().unwrap());
        let parsed = dec.next_row(2).unwrap().expect("row present");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0], Some(&value_bytes[..]));
        assert_eq!(parsed[1], None);
        // Next call returns the end-of-data.
        assert!(dec.next_row(2).unwrap().is_none());
        assert_eq!(dec.state(), BinaryState::EndOfData);
    }

    // ─── BinaryDecoder — header parsing ────────────────────────────────

    /// SP-PG-COPY-BIN T1: `consume_header` on a valid header transitions
    /// to Body and advances cursor by 19.
    #[test]
    fn t1_consume_header_valid_advances_cursor() {
        let mut h = encode_binary_header();
        h.extend_from_slice(&[1, 2, 3, 4]); // junk after header
        let mut dec = BinaryDecoder::new(&h);
        assert_eq!(dec.state(), BinaryState::Header);
        assert!(dec.consume_header().unwrap());
        assert_eq!(dec.state(), BinaryState::Body);
        assert_eq!(dec.cursor(), 19);
    }

    /// SP-PG-COPY-BIN T1: `consume_header` on a truncated buffer
    /// (less than 19 bytes) returns Ok(false) without advancing.
    #[test]
    fn t1_consume_header_truncated_returns_false() {
        let partial = &PG_BINARY_SIGNATURE[..7];
        let mut dec = BinaryDecoder::new(partial);
        assert_eq!(dec.consume_header(), Ok(false));
        assert_eq!(dec.state(), BinaryState::Header);
        assert_eq!(dec.cursor(), 0);
    }

    /// SP-PG-COPY-BIN T1: `consume_header` on a bad signature returns
    /// BadSignature.
    #[test]
    fn t1_consume_header_bad_signature_rejected() {
        let mut bad: Vec<u8> = Vec::new();
        bad.extend_from_slice(b"NOTPGCOPY!!");
        bad.extend_from_slice(&[0u8; 8]); // flags + ext_len
        let mut dec = BinaryDecoder::new(&bad);
        assert_eq!(dec.consume_header(), Err(BinaryDecodeError::BadSignature));
    }

    /// SP-PG-COPY-BIN T1: `consume_header` on non-zero flags rejects
    /// with UnsupportedFlags carrying the flag value.
    #[test]
    fn t1_consume_header_non_zero_flags_rejected() {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(PG_BINARY_SIGNATURE);
        // Flag bit 16 set (the legacy OID-column flag).
        buf.extend_from_slice(&0x00010000u32.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes()); // ext len
        let mut dec = BinaryDecoder::new(&buf);
        match dec.consume_header() {
            Err(BinaryDecodeError::UnsupportedFlags { flags }) => {
                assert_eq!(flags, 0x00010000);
            }
            other => panic!("expected UnsupportedFlags, got {other:?}"),
        }
    }

    /// SP-PG-COPY-BIN T1: a header extension area > MAX_COPY_DATA_BUFFER
    /// is rejected. (Using a value that fits in u32 but exceeds the
    /// 16 MiB cap.)
    #[test]
    fn t1_consume_header_oversized_extension_rejected() {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(PG_BINARY_SIGNATURE);
        buf.extend_from_slice(&0u32.to_be_bytes()); // flags
        // ext_len = 32 MiB > 16 MiB cap.
        buf.extend_from_slice(&(32 * 1024 * 1024u32).to_be_bytes());
        let mut dec = BinaryDecoder::new(&buf);
        match dec.consume_header() {
            Err(BinaryDecodeError::HeaderExtensionTooLarge { length }) => {
                assert_eq!(length, 32 * 1024 * 1024);
            }
            other => panic!("expected HeaderExtensionTooLarge, got {other:?}"),
        }
    }

    /// SP-PG-COPY-BIN T1: a header advertising a 4-byte extension is
    /// consumed in full when those bytes are present.
    #[test]
    fn t1_consume_header_with_extension_advances_past_extension() {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(PG_BINARY_SIGNATURE);
        buf.extend_from_slice(&0u32.to_be_bytes()); // flags
        buf.extend_from_slice(&4u32.to_be_bytes()); // ext len = 4
        buf.extend_from_slice(&[1, 2, 3, 4]); // extension bytes
        buf.extend_from_slice(&encode_binary_end_of_data()); // trailing eod
        let mut dec = BinaryDecoder::new(&buf);
        assert!(dec.consume_header().unwrap());
        assert_eq!(dec.cursor(), 19 + 4);
        assert_eq!(dec.state(), BinaryState::Body);
    }

    /// SP-PG-COPY-BIN T1: a header advertising a 4-byte extension but
    /// with only 2 of those bytes present returns Ok(false) (need more).
    #[test]
    fn t1_consume_header_with_partial_extension_needs_more() {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(PG_BINARY_SIGNATURE);
        buf.extend_from_slice(&0u32.to_be_bytes());
        buf.extend_from_slice(&4u32.to_be_bytes());
        buf.extend_from_slice(&[1, 2]); // only 2 of 4 extension bytes
        let mut dec = BinaryDecoder::new(&buf);
        assert_eq!(dec.consume_header(), Ok(false));
        assert_eq!(dec.state(), BinaryState::Header);
    }

    // ─── BinaryDecoder — row parsing ──────────────────────────────────

    /// SP-PG-COPY-BIN T1: an empty stream (header + end-of-data only)
    /// parses zero rows.
    #[test]
    fn t1_decode_empty_stream_yields_zero_rows() {
        let mut buf = encode_binary_header();
        buf.extend_from_slice(&encode_binary_end_of_data());
        let mut dec = BinaryDecoder::new(&buf);
        assert!(dec.consume_header().unwrap());
        let r = dec.next_row(1).unwrap();
        assert!(r.is_none());
        assert_eq!(dec.state(), BinaryState::EndOfData);
    }

    /// SP-PG-COPY-BIN T1: decoding a row with mismatched field count
    /// returns FieldCountMismatch.
    #[test]
    fn t1_decode_field_count_mismatch() {
        let int8_bytes: [u8; 8] = 42i64.to_be_bytes();
        let row = encode_binary_row(&[Some(&int8_bytes)]);
        let mut buf = encode_binary_header();
        buf.extend_from_slice(&row);
        let mut dec = BinaryDecoder::new(&buf);
        assert!(dec.consume_header().unwrap());
        // Expect 2 columns but only 1 in the row.
        match dec.next_row(2) {
            Err(BinaryDecodeError::FieldCountMismatch { expected, actual }) => {
                assert_eq!(expected, 2);
                assert_eq!(actual, 1);
            }
            other => panic!("expected FieldCountMismatch, got {other:?}"),
        }
    }

    /// SP-PG-COPY-BIN T1: a truncated row (only field count + half a
    /// length prefix) returns Ok(None) — need-more-data.
    #[test]
    fn t1_decode_truncated_mid_field_length_needs_more() {
        let mut buf = encode_binary_header();
        // Start of row: field count = 1, then only 2 of 4 length bytes.
        buf.extend_from_slice(&1i16.to_be_bytes());
        buf.extend_from_slice(&[0, 0]);
        let mut dec = BinaryDecoder::new(&buf);
        assert!(dec.consume_header().unwrap());
        assert_eq!(dec.next_row(1), Ok(None));
        // Cursor stayed at the start of the row (didn't advance past
        // the partial bytes — the caller will carry from header end).
        assert_eq!(dec.cursor(), 19);
    }

    /// SP-PG-COPY-BIN T1: a truncated row (full length prefix but
    /// not enough value bytes) returns Ok(None).
    #[test]
    fn t1_decode_truncated_mid_value_needs_more() {
        let mut buf = encode_binary_header();
        // Row: field count = 1, length = 8, only 4 of 8 value bytes.
        buf.extend_from_slice(&1i16.to_be_bytes());
        buf.extend_from_slice(&8i32.to_be_bytes());
        buf.extend_from_slice(&[0, 0, 0, 0]);
        let mut dec = BinaryDecoder::new(&buf);
        assert!(dec.consume_header().unwrap());
        assert_eq!(dec.next_row(1), Ok(None));
        assert_eq!(dec.cursor(), 19);
    }

    /// SP-PG-COPY-BIN T1: a row with a negative field-length (other
    /// than -1) is rejected with BadFieldLength.
    #[test]
    fn t1_decode_bad_field_length_rejected() {
        let mut buf = encode_binary_header();
        buf.extend_from_slice(&1i16.to_be_bytes());
        buf.extend_from_slice(&(-7i32).to_be_bytes()); // -7 is bad
        let mut dec = BinaryDecoder::new(&buf);
        assert!(dec.consume_header().unwrap());
        match dec.next_row(1) {
            Err(BinaryDecodeError::BadFieldLength { length }) => {
                assert_eq!(length, -7);
            }
            other => panic!("expected BadFieldLength, got {other:?}"),
        }
    }

    /// SP-PG-COPY-BIN T1: encode + decode round-trip for INT2/INT4/INT8.
    #[test]
    fn t1_round_trip_int_widths() {
        let i2: [u8; 2] = 1234i16.to_be_bytes();
        let i4: [u8; 4] = 56789i32.to_be_bytes();
        let i8: [u8; 8] = 99999999999i64.to_be_bytes();
        let row = encode_binary_row(&[Some(&i2), Some(&i4), Some(&i8)]);
        let mut buf = encode_binary_header();
        buf.extend_from_slice(&row);
        buf.extend_from_slice(&encode_binary_end_of_data());
        let mut dec = BinaryDecoder::new(&buf);
        assert!(dec.consume_header().unwrap());
        let r = dec.next_row(3).unwrap().expect("row");
        assert_eq!(r[0], Some(&i2[..]));
        assert_eq!(r[1], Some(&i4[..]));
        assert_eq!(r[2], Some(&i8[..]));
    }

    /// SP-PG-COPY-BIN T1: encode + decode round-trip for FLOAT4/FLOAT8.
    #[test]
    fn t1_round_trip_float_widths() {
        let f4: [u8; 4] = 1.5f32.to_be_bytes();
        let f8: [u8; 8] = std::f64::consts::PI.to_be_bytes();
        let row = encode_binary_row(&[Some(&f4), Some(&f8)]);
        let mut buf = encode_binary_header();
        buf.extend_from_slice(&row);
        let mut dec = BinaryDecoder::new(&buf);
        assert!(dec.consume_header().unwrap());
        let r = dec.next_row(2).unwrap().expect("row");
        assert_eq!(r[0], Some(&f4[..]));
        assert_eq!(r[1], Some(&f8[..]));
    }

    /// SP-PG-COPY-BIN T1: BOOL round-trip (single 0x00/0x01 byte).
    #[test]
    fn t1_round_trip_bool() {
        let row1 = encode_binary_row(&[Some(&[0x01])]);
        let row2 = encode_binary_row(&[Some(&[0x00])]);
        let mut buf = encode_binary_header();
        buf.extend_from_slice(&row1);
        buf.extend_from_slice(&row2);
        buf.extend_from_slice(&encode_binary_end_of_data());
        let mut dec = BinaryDecoder::new(&buf);
        assert!(dec.consume_header().unwrap());
        let r1 = dec.next_row(1).unwrap().expect("row1");
        assert_eq!(r1[0], Some(&[0x01u8][..]));
        let r2 = dec.next_row(1).unwrap().expect("row2");
        assert_eq!(r2[0], Some(&[0x00u8][..]));
        assert!(dec.next_row(1).unwrap().is_none());
        assert_eq!(dec.state(), BinaryState::EndOfData);
    }

    /// SP-PG-COPY-BIN T1: TEXT with multi-byte UTF-8 chars passes
    /// through verbatim.
    #[test]
    fn t1_round_trip_text_multibyte() {
        let text = "héllo, wörld 🌍";
        let row = encode_binary_row(&[Some(text.as_bytes())]);
        let mut buf = encode_binary_header();
        buf.extend_from_slice(&row);
        let mut dec = BinaryDecoder::new(&buf);
        assert!(dec.consume_header().unwrap());
        let r = dec.next_row(1).unwrap().expect("row");
        assert_eq!(r[0], Some(text.as_bytes()));
    }

    /// SP-PG-COPY-BIN T1: BYTEA with embedded zero bytes round-trips.
    /// (Binary BYTEA is raw bytes — no `\x` prefix, no hex encoding.)
    #[test]
    fn t1_round_trip_bytea_with_zero_bytes() {
        let raw: [u8; 6] = [0xDE, 0xAD, 0x00, 0xBE, 0xEF, 0x00];
        let row = encode_binary_row(&[Some(&raw)]);
        let mut buf = encode_binary_header();
        buf.extend_from_slice(&row);
        let mut dec = BinaryDecoder::new(&buf);
        assert!(dec.consume_header().unwrap());
        let r = dec.next_row(1).unwrap().expect("row");
        assert_eq!(r[0], Some(&raw[..]));
    }

    /// SP-PG-COPY-BIN T1: TIMESTAMPTZ round-trips its 8-byte BE i64
    /// payload through the codec verbatim.
    #[test]
    fn t1_round_trip_timestamptz() {
        // 26 years after PG epoch in microseconds (rough).
        let micros: i64 = 819_936_000_000_000;
        let ts: [u8; 8] = micros.to_be_bytes();
        let row = encode_binary_row(&[Some(&ts)]);
        let mut buf = encode_binary_header();
        buf.extend_from_slice(&row);
        let mut dec = BinaryDecoder::new(&buf);
        assert!(dec.consume_header().unwrap());
        let r = dec.next_row(1).unwrap().expect("row");
        assert_eq!(r[0], Some(&ts[..]));
        // Confirm we can recover the i64.
        let bytes = r[0].unwrap();
        let recovered = i64::from_be_bytes(bytes.try_into().unwrap());
        assert_eq!(recovered, micros);
    }

    /// SP-PG-COPY-BIN T1: empty rows (zero columns per row) round-trip.
    /// Locked because `field_count = 0` is wire-distinct from
    /// `field_count = -1` (end-of-data marker).
    #[test]
    fn t1_round_trip_zero_column_row() {
        let row = encode_binary_row(&[]);
        assert_eq!(row, vec![0x00, 0x00]); // field_count = 0
        let mut buf = encode_binary_header();
        buf.extend_from_slice(&row);
        buf.extend_from_slice(&encode_binary_end_of_data());
        let mut dec = BinaryDecoder::new(&buf);
        assert!(dec.consume_header().unwrap());
        let r = dec.next_row(0).unwrap().expect("zero-col row");
        assert!(r.is_empty());
        assert!(dec.next_row(0).unwrap().is_none());
        assert_eq!(dec.state(), BinaryState::EndOfData);
    }

    /// SP-PG-COPY-BIN T1: a stream with multiple rows + end-of-data
    /// yields all rows in order then transitions to EndOfData.
    #[test]
    fn t1_round_trip_three_rows_with_eod() {
        let r1 = encode_binary_row(&[Some(&1i64.to_be_bytes())]);
        let r2 = encode_binary_row(&[Some(&2i64.to_be_bytes())]);
        let r3 = encode_binary_row(&[Some(&3i64.to_be_bytes())]);
        let mut buf = encode_binary_header();
        buf.extend_from_slice(&r1);
        buf.extend_from_slice(&r2);
        buf.extend_from_slice(&r3);
        buf.extend_from_slice(&encode_binary_end_of_data());
        let mut dec = BinaryDecoder::new(&buf);
        assert!(dec.consume_header().unwrap());
        let v1 = dec.next_row(1).unwrap().expect("r1");
        assert_eq!(v1[0], Some(&1i64.to_be_bytes()[..]));
        let v2 = dec.next_row(1).unwrap().expect("r2");
        assert_eq!(v2[0], Some(&2i64.to_be_bytes()[..]));
        let v3 = dec.next_row(1).unwrap().expect("r3");
        assert_eq!(v3[0], Some(&3i64.to_be_bytes()[..]));
        assert!(dec.next_row(1).unwrap().is_none());
        assert_eq!(dec.state(), BinaryState::EndOfData);
    }

    /// SP-PG-COPY-BIN T1: `new_in_body` skips header consumption — used
    /// when the caller resumes a binary COPY across CopyData frame
    /// boundaries (state flag in `CopyInState` says the header is
    /// already consumed).
    #[test]
    fn t1_new_in_body_skips_header_consumption() {
        let r1 = encode_binary_row(&[Some(&42i64.to_be_bytes())]);
        let mut buf = r1.clone();
        buf.extend_from_slice(&encode_binary_end_of_data());
        let mut dec = BinaryDecoder::new_in_body(&buf);
        assert_eq!(dec.state(), BinaryState::Body);
        let r = dec.next_row(1).unwrap().expect("row");
        assert_eq!(r[0], Some(&42i64.to_be_bytes()[..]));
        assert!(dec.next_row(1).unwrap().is_none());
        assert_eq!(dec.state(), BinaryState::EndOfData);
    }
}
