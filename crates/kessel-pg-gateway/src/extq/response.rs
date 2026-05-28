//! Extended-Query backend-message encoders.
//!
//! **T1 status (this commit):** six encoders the Extended-Query
//! dispatcher needs that V1's `response.rs` / `error.rs` did NOT
//! already ship.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
//!
//! | Tag | Encoder | Wire size | Notes |
//! |---|---|---|---|
//! | `1` ParseComplete | `encode_parse_complete()` | 5 bytes | empty body |
//! | `2` BindComplete | `encode_bind_complete()` | 5 bytes | empty body |
//! | `3` CloseComplete | `encode_close_complete()` | 5 bytes | empty body |
//! | `n` NoData | `encode_no_data()` | 5 bytes | empty body |
//! | `s` PortalSuspended | `encode_portal_suspended()` | 5 bytes | empty body |
//! | `t` ParameterDescription | `encode_parameter_description(oids)` | 7 + 4·N | `[count:i16] [oid:i32]*` |
//!
//! Five of the six are trivial type-byte + 4-byte length envelopes
//! (`tag [length=4]`). They're each locked individually as KATs
//! because byte-flip regressions would silently break every PG
//! client — these messages are how the client knows the previous
//! command succeeded.
//!
//! `RowDescription` ('T') / `DataRow` ('D') / `CommandComplete` ('C')
//! / `ReadyForQuery` ('Z') / `ErrorResponse` ('E') / `EmptyQueryResponse`
//! ('I') already live in V1's `response.rs` + `error.rs` and are
//! re-used by the Extended-Query path unchanged.

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::proto::{
    BE_BIND_COMPLETE, BE_CLOSE_COMPLETE, BE_NO_DATA, BE_PARAMETER_DESCRIPTION,
    BE_PARSE_COMPLETE, BE_PORTAL_SUSPENDED,
};

/// `1` ParseComplete — wire: `1 [length:4 BE = 4]`. PG §55.7.
/// Server emits after a successful Parse (T2).
pub fn encode_parse_complete() -> Vec<u8> {
    vec![BE_PARSE_COMPLETE, 0, 0, 0, 4]
}

/// `2` BindComplete — wire: `2 [length:4 BE = 4]`. PG §55.7.
/// Server emits after a successful Bind (T3).
pub fn encode_bind_complete() -> Vec<u8> {
    vec![BE_BIND_COMPLETE, 0, 0, 0, 4]
}

/// `3` CloseComplete — wire: `3 [length:4 BE = 4]`. PG §55.7.
/// Server emits after a successful Close (T8).
pub fn encode_close_complete() -> Vec<u8> {
    vec![BE_CLOSE_COMPLETE, 0, 0, 0, 4]
}

/// `n` NoData — wire: `n [length:4 BE = 4]`. PG §55.7.
/// Server emits in response to Describe ('S' or 'P') for a statement
/// or portal that returns no rows (T4 / T5). Distinct from
/// RowDescription with 0 fields — the client uses NoData to short-
/// circuit row-decoding setup.
pub fn encode_no_data() -> Vec<u8> {
    vec![BE_NO_DATA, 0, 0, 0, 4]
}

/// `s` PortalSuspended — wire: `s [length:4 BE = 4]`. PG §55.7.
/// Server emits in place of CommandComplete when an Execute hits
/// its max_rows limit and there are more buffered rows (T9). The
/// client can re-Execute the same portal to continue.
pub fn encode_portal_suspended() -> Vec<u8> {
    vec![BE_PORTAL_SUSPENDED, 0, 0, 0, 4]
}

/// `t` ParameterDescription — wire: `t [length:4 BE]
/// [count:i16 BE] [oid:i32 BE]*`. PG §55.7. Server emits in response
/// to Describe 'S' (statement) — informs the client of the OID type
/// hints from Parse (if any) for each `$N` parameter (T4). For
/// statements with no parameters the encoder produces `count=0` and
/// the message is 7 bytes total.
///
/// Note: `oid` is an unsigned 32-bit OID per PG, but the wire field
/// is the same i32 BE encoding either way; we accept `u32` so the
/// caller can pass our `proto::PG_TYPE_*` constants verbatim.
pub fn encode_parameter_description(oids: &[u32]) -> Vec<u8> {
    let payload_len = 2 + oids.len() * 4; // u16 count + 4*N oids
    let total_length = (4 + payload_len) as u32;
    let mut frame = Vec::with_capacity(1 + total_length as usize);
    frame.push(BE_PARAMETER_DESCRIPTION);
    frame.extend_from_slice(&total_length.to_be_bytes());
    frame.extend_from_slice(&(oids.len() as u16).to_be_bytes());
    for oid in oids {
        frame.extend_from_slice(&oid.to_be_bytes());
    }
    frame
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{PG_TYPE_INT4, PG_TYPE_INT8, PG_TYPE_TEXT};

    // ───────────────────────────────────────────────────────────────────
    // T1 KATs — every encoder byte-locked vs the PG §55.7 canonical
    // shape. A 1-byte drift here would silently break every PG client
    // because the type byte + length field is how PG clients
    // distinguish ParseComplete from BindComplete from CloseComplete
    // (they're all the same 5-byte shape with only the type byte
    // differing).
    // ───────────────────────────────────────────────────────────────────

    #[test]
    fn t1_parse_complete_is_five_bytes_one_tag_then_length_four() {
        let frame = encode_parse_complete();
        assert_eq!(frame, vec![b'1', 0, 0, 0, 4]);
        assert_eq!(frame.len(), 5);
    }

    #[test]
    fn t1_bind_complete_is_five_bytes_two_tag_then_length_four() {
        let frame = encode_bind_complete();
        assert_eq!(frame, vec![b'2', 0, 0, 0, 4]);
        assert_eq!(frame.len(), 5);
    }

    #[test]
    fn t1_close_complete_is_five_bytes_three_tag_then_length_four() {
        let frame = encode_close_complete();
        assert_eq!(frame, vec![b'3', 0, 0, 0, 4]);
        assert_eq!(frame.len(), 5);
    }

    #[test]
    fn t1_no_data_is_five_bytes_lowercase_n_tag_then_length_four() {
        let frame = encode_no_data();
        assert_eq!(frame, vec![b'n', 0, 0, 0, 4]);
        assert_eq!(frame.len(), 5);
    }

    #[test]
    fn t1_portal_suspended_is_five_bytes_lowercase_s_tag_then_length_four() {
        let frame = encode_portal_suspended();
        assert_eq!(frame, vec![b's', 0, 0, 0, 4]);
        assert_eq!(frame.len(), 5);
    }

    #[test]
    fn t1_parameter_description_empty_is_seven_bytes_with_count_zero() {
        let frame = encode_parameter_description(&[]);
        // 't' + length=6 + count=0
        assert_eq!(frame, vec![b't', 0, 0, 0, 6, 0, 0]);
        assert_eq!(frame.len(), 7);
    }

    #[test]
    fn t1_parameter_description_single_int8_oid_byte_locked() {
        let frame = encode_parameter_description(&[PG_TYPE_INT8]);
        // 't' + length=10 + count=1 + oid=20 (int8)
        let mut expected = Vec::new();
        expected.push(b't');
        expected.extend_from_slice(&10u32.to_be_bytes()); // length = 4 + 2 + 4 = 10
        expected.extend_from_slice(&1u16.to_be_bytes());
        expected.extend_from_slice(&20u32.to_be_bytes());
        assert_eq!(frame, expected);
        assert_eq!(frame.len(), 11);
    }

    #[test]
    fn t1_parameter_description_multiple_oids_byte_locked() {
        let frame =
            encode_parameter_description(&[PG_TYPE_INT4, PG_TYPE_TEXT, PG_TYPE_INT8]);
        // 't' + length=18 + count=3 + oid×3
        let mut expected = Vec::new();
        expected.push(b't');
        expected.extend_from_slice(&18u32.to_be_bytes()); // 4 + 2 + 12 = 18
        expected.extend_from_slice(&3u16.to_be_bytes());
        expected.extend_from_slice(&PG_TYPE_INT4.to_be_bytes());
        expected.extend_from_slice(&PG_TYPE_TEXT.to_be_bytes());
        expected.extend_from_slice(&PG_TYPE_INT8.to_be_bytes());
        assert_eq!(frame, expected);
    }

    /// Tag bytes are distinct across the trivial-envelope encoders.
    /// A byte-flip refactor that confused ParseComplete with
    /// BindComplete would silently corrupt every prepared-statement
    /// flow; this KAT catches it.
    #[test]
    fn t1_extq_trivial_envelope_tags_are_distinct() {
        let tags: Vec<u8> = vec![
            encode_parse_complete()[0],
            encode_bind_complete()[0],
            encode_close_complete()[0],
            encode_no_data()[0],
            encode_portal_suspended()[0],
        ];
        let unique: std::collections::HashSet<u8> = tags.iter().copied().collect();
        assert_eq!(unique.len(), tags.len(), "tags must be distinct");
        // Confirmed values for crash-safety against drift.
        assert_eq!(tags, vec![b'1', b'2', b'3', b'n', b's']);
    }

    /// All trivial-envelope encoders produce the SAME length field
    /// (4 = "length includes itself, no payload"). Locked because a
    /// refactor that accidentally added a payload byte would shift
    /// every subsequent message on the wire by one byte and break
    /// every client's framing.
    #[test]
    fn t1_extq_trivial_envelope_lengths_are_all_four() {
        for f in &[
            encode_parse_complete(),
            encode_bind_complete(),
            encode_close_complete(),
            encode_no_data(),
            encode_portal_suspended(),
        ] {
            // bytes [1..5] are the BE u32 length field
            let length = u32::from_be_bytes([f[1], f[2], f[3], f[4]]);
            assert_eq!(length, 4);
            assert_eq!(f.len(), 5);
        }
    }
}
