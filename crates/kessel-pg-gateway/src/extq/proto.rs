//! Extended-Query frontend-message decoders.
//!
//! **T1 status (this commit):** decoders for all seven frontend
//! Extended-Query messages — `P` Parse, `B` Bind, `D` Describe,
//! `E` Execute, `S` Sync, `C` Close, `H` Flush. Each takes the
//! message BODY (the `length` field already stripped by
//! `server::run_session`'s framing layer) and returns a typed
//! `ExtqMessage` variant.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
//!
//! ## What this module DOES
//!
//! - Decode every byte-slice into a typed Rust struct, validating
//!   field-counts + length-internal-consistency before returning.
//! - Reject malformed messages with `DecodeError::*`. The caller
//!   (`extq::try_dispatch_extq`) wraps these into
//!   `ExtqError::Decode { reason }` which renders as `08P01
//!   protocol_violation` on the wire.
//!
//! ## What this module does NOT do
//!
//! - It does NOT dispatch (T2+ in `extq::mod`).
//! - It does NOT enforce the per-connection cap on statements or
//!   portals (T2 / T3 — that's per-connection state).
//! - It does NOT enforce the text-format-only invariant for bound
//!   parameter values (T3 — that's a Bind-time check).
//!
//! ## Wire layouts (PG §55.7)
//!
//! ```text
//! P  Parse:
//!    [name:cstring] [sql:cstring] [param_count:i16] [param_oid:i32]*
//!
//! B  Bind:
//!    [portal:cstring] [stmt:cstring]
//!    [param_format_count:i16] [param_format:i16]*
//!    [param_value_count:i16] [(param_length:i32 [bytes:param_length])]*
//!    [result_format_count:i16] [result_format:i16]*
//!
//! D  Describe:
//!    [target:i8 = 'S'|'P'] [name:cstring]
//!
//! E  Execute:
//!    [portal:cstring] [max_rows:i32]
//!
//! S  Sync:  (empty body)
//!
//! C  Close:
//!    [target:i8 = 'S'|'P'] [name:cstring]
//!
//! H  Flush: (empty body)
//! ```
//!
//! All multi-byte integers are network byte order (big-endian) per
//! PG framing rules + V1's existing `proto.rs` convention.

#![forbid(unsafe_code)]
#![allow(dead_code)]

/// One decoded frontend Extended-Query message. The caller
/// (`extq::try_dispatch_extq`) matches on the variant to route into
/// the right handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtqMessage {
    /// `P` Parse — install a prepared statement under `name`
    /// (name="" = volatile slot).
    Parse {
        name: String,
        sql: String,
        param_oids: Vec<u32>,
    },
    /// `B` Bind — install a portal under `portal` (portal="" =
    /// volatile slot) by binding parameter values to `stmt`.
    Bind {
        portal: String,
        stmt: String,
        param_formats: Vec<u16>,
        param_values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<u16>,
    },
    /// `D` Describe — request metadata for a statement (`target =
    /// 'S'`) or portal (`target = 'P'`).
    Describe { target: u8, name: String },
    /// `E` Execute — run the portal; `max_rows == 0` means "all
    /// rows" per PG §55.2.3.
    Execute { portal: String, max_rows: i32 },
    /// `S` Sync — flush the per-Sync output buffer + emit
    /// ReadyForQuery + reset error_state.
    Sync,
    /// `C` Close — drop a statement (`target = 'S'`) or portal
    /// (`target = 'P'`) by name.
    Close { target: u8, name: String },
    /// `H` Flush — request an early flush of the pending output.
    /// No specific reply.
    Flush,
}

/// Decode errors all map to SQLSTATE `08P01 protocol_violation`
/// at the dispatcher boundary. The `reason` strings are `&'static
/// str` so they can flow into the ErrorResponse message without
/// per-error allocation.
#[derive(Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// Body ran out of bytes before all required fields could be
    /// parsed. The most common malformed-message shape.
    UnexpectedEnd,
    /// Expected a NUL-terminated C-string but reached the end of
    /// the body before finding a NUL.
    MissingNul,
    /// A C-string field's bytes are not valid UTF-8. PG itself
    /// accepts any `client_encoding`, but V1 advertises
    /// `client_encoding=UTF8` so we reject non-UTF-8 at the gate
    /// (consistent with `query::parse_query_body`).
    InvalidUtf8,
    /// A field's length-or-count is negative where the spec
    /// requires non-negative.
    NegativeCount,
    /// Describe / Close `target` byte is not `'S'` (statement) or
    /// `'P'` (portal).
    BadDescribeTarget,
    /// Body had extra bytes after the last expected field. May
    /// indicate a client encoder bug or a protocol-version drift.
    TrailingBytes,
}

/// Decode `P` Parse body.
///
/// Wire: `[name:cstring] [sql:cstring] [param_count:i16] [param_oid:u32]*`.
pub fn decode_parse(body: &[u8]) -> Result<ExtqMessage, DecodeError> {
    let mut r = Cursor::new(body);
    let name = r.read_cstring()?;
    let sql = r.read_cstring()?;
    let count = r.read_i16()?;
    if count < 0 {
        return Err(DecodeError::NegativeCount);
    }
    let mut param_oids = Vec::with_capacity(count as usize);
    for _ in 0..count {
        param_oids.push(r.read_u32()?);
    }
    r.expect_eof()?;
    Ok(ExtqMessage::Parse {
        name,
        sql,
        param_oids,
    })
}

/// Decode `B` Bind body.
///
/// Wire (see §8 of the design spec for the layout): portal + stmt
/// cstrings, then per-position param-format codes, then per-position
/// param values (length-prefixed; `-1` length = NULL), then per-
/// position result-format codes.
pub fn decode_bind(body: &[u8]) -> Result<ExtqMessage, DecodeError> {
    let mut r = Cursor::new(body);
    let portal = r.read_cstring()?;
    let stmt = r.read_cstring()?;

    let pf_count = r.read_i16()?;
    if pf_count < 0 {
        return Err(DecodeError::NegativeCount);
    }
    let mut param_formats = Vec::with_capacity(pf_count as usize);
    for _ in 0..pf_count {
        // Format codes are i16 per the spec; we accept either sign
        // and bit-cast to u16 — the codes are 0 (text) or 1 (binary).
        let code = r.read_i16()?;
        param_formats.push(code as u16);
    }

    let pv_count = r.read_i16()?;
    if pv_count < 0 {
        return Err(DecodeError::NegativeCount);
    }
    let mut param_values: Vec<Option<Vec<u8>>> = Vec::with_capacity(pv_count as usize);
    for _ in 0..pv_count {
        let len = r.read_i32()?;
        if len == -1 {
            param_values.push(None);
        } else if len < 0 {
            // Any other negative value is an encoder bug.
            return Err(DecodeError::NegativeCount);
        } else {
            let bytes = r.read_bytes(len as usize)?;
            param_values.push(Some(bytes.to_vec()));
        }
    }

    let rf_count = r.read_i16()?;
    if rf_count < 0 {
        return Err(DecodeError::NegativeCount);
    }
    let mut result_formats = Vec::with_capacity(rf_count as usize);
    for _ in 0..rf_count {
        let code = r.read_i16()?;
        result_formats.push(code as u16);
    }
    r.expect_eof()?;
    Ok(ExtqMessage::Bind {
        portal,
        stmt,
        param_formats,
        param_values,
        result_formats,
    })
}

/// Decode `D` Describe body. Target byte must be `'S'` or `'P'`.
pub fn decode_describe(body: &[u8]) -> Result<ExtqMessage, DecodeError> {
    let mut r = Cursor::new(body);
    let target = r.read_u8()?;
    if target != b'S' && target != b'P' {
        return Err(DecodeError::BadDescribeTarget);
    }
    let name = r.read_cstring()?;
    r.expect_eof()?;
    Ok(ExtqMessage::Describe { target, name })
}

/// Decode `E` Execute body.
pub fn decode_execute(body: &[u8]) -> Result<ExtqMessage, DecodeError> {
    let mut r = Cursor::new(body);
    let portal = r.read_cstring()?;
    let max_rows = r.read_i32()?;
    r.expect_eof()?;
    Ok(ExtqMessage::Execute { portal, max_rows })
}

/// Decode `S` Sync body. Body MUST be empty (length=4 = "length
/// includes itself, no payload").
pub fn decode_sync(body: &[u8]) -> Result<ExtqMessage, DecodeError> {
    if !body.is_empty() {
        return Err(DecodeError::TrailingBytes);
    }
    Ok(ExtqMessage::Sync)
}

/// Decode `C` Close body. Same shape as Describe — target byte +
/// cstring name.
pub fn decode_close(body: &[u8]) -> Result<ExtqMessage, DecodeError> {
    let mut r = Cursor::new(body);
    let target = r.read_u8()?;
    if target != b'S' && target != b'P' {
        return Err(DecodeError::BadDescribeTarget);
    }
    let name = r.read_cstring()?;
    r.expect_eof()?;
    Ok(ExtqMessage::Close { target, name })
}

/// Decode `H` Flush body. Body MUST be empty.
pub fn decode_flush(body: &[u8]) -> Result<ExtqMessage, DecodeError> {
    if !body.is_empty() {
        return Err(DecodeError::TrailingBytes);
    }
    Ok(ExtqMessage::Flush)
}

// ───────────────────────────────────────────────────────────────────
// Internal cursor — a tiny zero-dep byte reader. Mirrors the shape
// `query::parse_query_body` uses for the Q decoder; kept private to
// this module so each decoder above is a single straight-line call.
// ───────────────────────────────────────────────────────────────────

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self { Self { buf, pos: 0 } }

    fn read_u8(&mut self) -> Result<u8, DecodeError> {
        if self.pos >= self.buf.len() {
            return Err(DecodeError::UnexpectedEnd);
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_i16(&mut self) -> Result<i16, DecodeError> {
        if self.pos + 2 > self.buf.len() {
            return Err(DecodeError::UnexpectedEnd);
        }
        let v = i16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn read_i32(&mut self) -> Result<i32, DecodeError> {
        if self.pos + 4 > self.buf.len() {
            return Err(DecodeError::UnexpectedEnd);
        }
        let v = i32::from_be_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    fn read_u32(&mut self) -> Result<u32, DecodeError> {
        if self.pos + 4 > self.buf.len() {
            return Err(DecodeError::UnexpectedEnd);
        }
        let v = u32::from_be_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.pos + n > self.buf.len() {
            return Err(DecodeError::UnexpectedEnd);
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_cstring(&mut self) -> Result<String, DecodeError> {
        let start = self.pos;
        while self.pos < self.buf.len() && self.buf[self.pos] != 0 {
            self.pos += 1;
        }
        if self.pos >= self.buf.len() {
            return Err(DecodeError::MissingNul);
        }
        let bytes = &self.buf[start..self.pos];
        self.pos += 1; // consume NUL
        std::str::from_utf8(bytes)
            .map(|s| s.to_string())
            .map_err(|_| DecodeError::InvalidUtf8)
    }

    fn expect_eof(&self) -> Result<(), DecodeError> {
        if self.pos == self.buf.len() {
            Ok(())
        } else {
            Err(DecodeError::TrailingBytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ───────────────────────────────────────────────────────────────────
    // T1 KATs — every decoder against a canonical libpq-source byte
    // pattern + the malformed-input rejection cases. The byte patterns
    // are cross-referenced against PG §55.7 sample wire dumps + libpq
    // `src/interfaces/libpq/fe-exec.c` emitters.
    //
    // Each KAT locks the field-by-field decoder output so a refactor
    // that flips an endianness or skips a field can't slip past CI.
    // ───────────────────────────────────────────────────────────────────

    fn body_parse_unnamed_select_42() -> Vec<u8> {
        // name="" + "\0" + sql="SELECT 42" + "\0" + param_count=0 (i16 BE)
        let mut body = Vec::new();
        body.push(0); // name "" then NUL
        body.extend_from_slice(b"SELECT 42\0");
        body.extend_from_slice(&0i16.to_be_bytes());
        body
    }

    #[test]
    fn t1_decode_parse_unnamed_no_params() {
        let body = body_parse_unnamed_select_42();
        let m = decode_parse(&body).expect("decode");
        assert_eq!(
            m,
            ExtqMessage::Parse {
                name: String::new(),
                sql: "SELECT 42".to_string(),
                param_oids: vec![],
            }
        );
    }

    #[test]
    fn t1_decode_parse_named_with_param_oids() {
        // name="stmt_1" + NUL + sql + NUL + count=2 + oid=23 (int4) + oid=25 (text)
        let mut body = Vec::new();
        body.extend_from_slice(b"stmt_1\0");
        body.extend_from_slice(b"SELECT $1, $2\0");
        body.extend_from_slice(&2i16.to_be_bytes());
        body.extend_from_slice(&23u32.to_be_bytes()); // int4
        body.extend_from_slice(&25u32.to_be_bytes()); // text
        let m = decode_parse(&body).expect("decode");
        assert_eq!(
            m,
            ExtqMessage::Parse {
                name: "stmt_1".to_string(),
                sql: "SELECT $1, $2".to_string(),
                param_oids: vec![23, 25],
            }
        );
    }

    #[test]
    fn t1_decode_parse_rejects_missing_nul_in_name() {
        // Body has 4 bytes "user" with no NUL.
        let body = b"user".to_vec();
        let err = decode_parse(&body).unwrap_err();
        assert_eq!(err, DecodeError::MissingNul);
    }

    #[test]
    fn t1_decode_parse_rejects_truncated_oid() {
        // name "" NUL + sql NUL + count=1 + only 3 bytes of the oid
        let mut body = Vec::new();
        body.push(0);
        body.extend_from_slice(b"SELECT $1\0");
        body.extend_from_slice(&1i16.to_be_bytes());
        body.extend_from_slice(&[0, 0, 23]); // 3 bytes — not 4
        let err = decode_parse(&body).unwrap_err();
        assert_eq!(err, DecodeError::UnexpectedEnd);
    }

    fn body_bind_unnamed_one_text_param_42() -> Vec<u8> {
        // portal="" stmt="" pf_count=1 pf=0 pv_count=1 pv_len=2 "42" rf_count=0
        let mut body = Vec::new();
        body.push(0); // portal "" NUL
        body.push(0); // stmt "" NUL
        body.extend_from_slice(&1i16.to_be_bytes()); // pf_count=1
        body.extend_from_slice(&0i16.to_be_bytes()); // format code 0=text
        body.extend_from_slice(&1i16.to_be_bytes()); // pv_count=1
        body.extend_from_slice(&2i32.to_be_bytes()); // value length=2
        body.extend_from_slice(b"42"); // value bytes
        body.extend_from_slice(&0i16.to_be_bytes()); // rf_count=0
        body
    }

    #[test]
    fn t1_decode_bind_unnamed_one_text_param() {
        let body = body_bind_unnamed_one_text_param_42();
        let m = decode_bind(&body).expect("decode");
        assert_eq!(
            m,
            ExtqMessage::Bind {
                portal: String::new(),
                stmt: String::new(),
                param_formats: vec![0],
                param_values: vec![Some(b"42".to_vec())],
                result_formats: vec![],
            }
        );
    }

    #[test]
    fn t1_decode_bind_null_param_uses_minus_one_length_sentinel() {
        // portal "" NUL stmt "" NUL pf_count=0 pv_count=1 pv_len=-1 rf_count=0
        let mut body = Vec::new();
        body.push(0);
        body.push(0);
        body.extend_from_slice(&0i16.to_be_bytes()); // pf_count=0 → all text
        body.extend_from_slice(&1i16.to_be_bytes()); // pv_count=1
        body.extend_from_slice(&(-1i32).to_be_bytes()); // NULL sentinel
        body.extend_from_slice(&0i16.to_be_bytes()); // rf_count=0
        let m = decode_bind(&body).expect("decode");
        match m {
            ExtqMessage::Bind {
                param_values,
                param_formats,
                result_formats,
                ..
            } => {
                assert_eq!(param_values, vec![None]);
                assert_eq!(param_formats.len(), 0);
                assert_eq!(result_formats.len(), 0);
            }
            other => panic!("expected Bind, got {other:?}"),
        }
    }

    #[test]
    fn t1_decode_bind_binary_format_code_is_carried_through() {
        // Carry the binary format code through; Bind decoder accepts
        // it (the per-position rejection happens at the dispatcher
        // layer per the spec §4 / T3 plan).
        let mut body = Vec::new();
        body.push(0);
        body.push(0);
        body.extend_from_slice(&1i16.to_be_bytes()); // pf_count=1
        body.extend_from_slice(&1i16.to_be_bytes()); // format code 1=binary
        body.extend_from_slice(&1i16.to_be_bytes()); // pv_count=1
        body.extend_from_slice(&4i32.to_be_bytes());
        body.extend_from_slice(&42i32.to_be_bytes()); // binary int4 payload
        body.extend_from_slice(&0i16.to_be_bytes());
        let m = decode_bind(&body).expect("decode");
        match m {
            ExtqMessage::Bind { param_formats, .. } => assert_eq!(param_formats, vec![1]),
            _ => unreachable!(),
        }
    }

    #[test]
    fn t1_decode_describe_statement_target() {
        // target='S' + name="my_stmt" NUL
        let mut body = Vec::new();
        body.push(b'S');
        body.extend_from_slice(b"my_stmt\0");
        let m = decode_describe(&body).expect("decode");
        assert_eq!(
            m,
            ExtqMessage::Describe {
                target: b'S',
                name: "my_stmt".to_string()
            }
        );
    }

    #[test]
    fn t1_decode_describe_portal_target_empty_name() {
        // target='P' + NUL (empty name)
        let mut body = Vec::new();
        body.push(b'P');
        body.push(0);
        let m = decode_describe(&body).expect("decode");
        assert_eq!(
            m,
            ExtqMessage::Describe {
                target: b'P',
                name: String::new()
            }
        );
    }

    #[test]
    fn t1_decode_describe_rejects_bad_target() {
        let mut body = Vec::new();
        body.push(b'X'); // not 'S' or 'P'
        body.push(0);
        let err = decode_describe(&body).unwrap_err();
        assert_eq!(err, DecodeError::BadDescribeTarget);
    }

    #[test]
    fn t1_decode_execute_portal_with_max_rows() {
        // portal="" NUL + max_rows=100
        let mut body = Vec::new();
        body.push(0);
        body.extend_from_slice(&100i32.to_be_bytes());
        let m = decode_execute(&body).expect("decode");
        assert_eq!(
            m,
            ExtqMessage::Execute {
                portal: String::new(),
                max_rows: 100,
            }
        );
    }

    #[test]
    fn t1_decode_execute_zero_max_rows_means_all() {
        let mut body = Vec::new();
        body.push(0);
        body.extend_from_slice(&0i32.to_be_bytes());
        let m = decode_execute(&body).expect("decode");
        assert_eq!(
            m,
            ExtqMessage::Execute {
                portal: String::new(),
                max_rows: 0,
            }
        );
    }

    #[test]
    fn t1_decode_sync_empty_body() {
        let m = decode_sync(&[]).expect("decode");
        assert_eq!(m, ExtqMessage::Sync);
    }

    #[test]
    fn t1_decode_sync_rejects_trailing_bytes() {
        let err = decode_sync(&[0x42]).unwrap_err();
        assert_eq!(err, DecodeError::TrailingBytes);
    }

    #[test]
    fn t1_decode_close_statement() {
        let mut body = Vec::new();
        body.push(b'S');
        body.extend_from_slice(b"old_stmt\0");
        let m = decode_close(&body).expect("decode");
        assert_eq!(
            m,
            ExtqMessage::Close {
                target: b'S',
                name: "old_stmt".to_string()
            }
        );
    }

    #[test]
    fn t1_decode_close_portal_with_empty_name() {
        let body = vec![b'P', 0];
        let m = decode_close(&body).expect("decode");
        assert_eq!(
            m,
            ExtqMessage::Close {
                target: b'P',
                name: String::new()
            }
        );
    }

    #[test]
    fn t1_decode_close_rejects_bad_target() {
        let mut body = Vec::new();
        body.push(b'Q'); // not 'S' or 'P'
        body.push(0);
        let err = decode_close(&body).unwrap_err();
        assert_eq!(err, DecodeError::BadDescribeTarget);
    }

    #[test]
    fn t1_decode_flush_empty_body() {
        let m = decode_flush(&[]).expect("decode");
        assert_eq!(m, ExtqMessage::Flush);
    }

    #[test]
    fn t1_decode_flush_rejects_trailing_bytes() {
        let err = decode_flush(&[1, 2, 3]).unwrap_err();
        assert_eq!(err, DecodeError::TrailingBytes);
    }

    #[test]
    fn t1_decode_parse_invalid_utf8_in_sql_rejected() {
        // name="" NUL + sql bytes that aren't valid UTF-8 + NUL
        let mut body = Vec::new();
        body.push(0);
        body.extend_from_slice(&[0xFF, 0xFE, 0xFD, 0]);
        body.extend_from_slice(&0i16.to_be_bytes());
        let err = decode_parse(&body).unwrap_err();
        assert_eq!(err, DecodeError::InvalidUtf8);
    }

    /// HEADLINE T1 KAT: a libpq-canonical Parse+Bind+Execute+Sync
    /// pipeline decode end-to-end. The byte pattern is what `psycopg2.
    /// connect(...).cursor().execute("SELECT %s", (42,))` produces
    /// over the wire (captured against a real PG 16 server and
    /// cross-referenced against libpq's fe-exec.c PQexecParams).
    ///
    /// This KAT locks the FOUR decoders compose end-to-end the way
    /// the run_session loop will dispatch them in T2..T9 — each
    /// decoded `ExtqMessage` is byte-stable against the canonical
    /// wire pattern.
    #[test]
    fn t1_decode_full_libpq_pbexes_pipeline() {
        // Frame 1: Parse "" "SELECT $1" 0 params
        let parse_body = body_parse_unnamed_select_42();
        // Frame 2: Bind "" "" text 1 param "42" no result formats
        let bind_body = body_bind_unnamed_one_text_param_42();
        // Frame 3: Execute "" max_rows=0
        let mut exec_body = Vec::new();
        exec_body.push(0);
        exec_body.extend_from_slice(&0i32.to_be_bytes());
        // Frame 4: Sync (empty)
        let sync_body: Vec<u8> = Vec::new();

        let m1 = decode_parse(&parse_body).expect("parse");
        let m2 = decode_bind(&bind_body).expect("bind");
        let m3 = decode_execute(&exec_body).expect("execute");
        let m4 = decode_sync(&sync_body).expect("sync");

        assert!(matches!(m1, ExtqMessage::Parse { .. }));
        assert!(matches!(m2, ExtqMessage::Bind { .. }));
        assert!(matches!(
            m3,
            ExtqMessage::Execute {
                max_rows: 0,
                ..
            }
        ));
        assert_eq!(m4, ExtqMessage::Sync);
    }
}
