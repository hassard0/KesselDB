//! Postgres Frontend/Backend protocol v3.0 — message-type tags +
//! protocol constants + framing rules.
//!
//! Locked here so the spec, the encoders (T5/T6/T7), the decoders
//! (T2/T3), and the tests can reference symbolic names instead of
//! magic bytes. Every constant in this module is cross-referenced
//! against the PostgreSQL §55 documentation (the authoritative source
//! for the v3.0 protocol).
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`
//!
//! ## Framing (spec §3.1)
//!
//! Every message after the StartupMessage / SSLRequest / CancelRequest
//! is:
//!
//! ```text
//! [type:1 byte][length:4 byte BE — includes itself but NOT type][payload]
//! ```
//!
//! The StartupMessage / SSLRequest / CancelRequest are the only
//! messages WITHOUT a type byte — they begin with the 4-byte BE
//! length directly. This is because they pre-date the v3 protocol's
//! type-byte discipline; libpq keeps it for back-compat.

#![forbid(unsafe_code)]
#![allow(dead_code)]

// ───────────────────────────────────────────────────────────────────────
// Protocol-version magic numbers (StartupMessage's first u32 payload)
// ───────────────────────────────────────────────────────────────────────

/// Spec §3.2: the only protocol version V1 accepts. 0x00030000 =
/// major 3, minor 0. libpq has spoken this since 2003; every modern
/// PG client defaults to it.
pub const PG_PROTOCOL_VERSION_3_0: u32 = 196608;

/// Spec §3 §3.2 / §8.6: pre-protocol-handshake magic for "client
/// wants TLS". libpq sends this BEFORE StartupMessage if the client
/// is configured with `sslmode=require`/`prefer`. V1 replies single
/// byte 'N' (no TLS — fall back to plaintext); V2 (when the `tls`
/// feature lands) will reply 'S' and perform the rustls handshake.
pub const PG_SSL_REQUEST_CODE: u32 = 80877103;

/// Spec §10 / §3 V1 disposition: pre-protocol-handshake magic for
/// "client wants to cancel a running query on another connection".
/// V1 logs and ignores; V2 will action via a process-wide cancel-key
/// table.
pub const PG_CANCEL_REQUEST_CODE: u32 = 80877102;

/// Spec §3: pre-protocol-handshake magic for GSSAPI request. V1 will
/// reply 'N' (same shape as SSLRequest) — GSSAPI is permanently out
/// of scope. Listed for completeness so the T2 startup parser can
/// distinguish it from a malformed StartupMessage.
pub const PG_GSS_ENC_REQUEST_CODE: u32 = 80877104;

// ───────────────────────────────────────────────────────────────────────
// Frontend (client → server) message-type tags (PG §55.2.1 / §55.7)
// ───────────────────────────────────────────────────────────────────────

/// Simple Query: `Q [length] [SQL text]\0`. V1 handles. Spec §3 / §4.
pub const FE_QUERY: u8 = b'Q';

/// Terminate: `X [length=4]`. V1 closes the connection on receipt.
/// Spec §3 / §8.4.
pub const FE_TERMINATE: u8 = b'X';

/// PasswordMessage / SASL response: `p [length] [payload]`. V1 uses
/// this for SCRAM client_first and client_final messages. Spec §3.3.
pub const FE_PASSWORD: u8 = b'p';

/// Extended Query — Parse: `P [length] [name\0] [SQL\0] [param_count
/// u16] [param_types u32]*`. V1 REJECTS with `0A000`
/// feature_not_supported. V2 (SP-PG-EXTQ) implements.
pub const FE_PARSE: u8 = b'P';

/// Extended Query — Bind: `B [length] [portal\0] [stmt\0]
/// [format_count u16] [formats u16]* [param_count u16]
/// [param_values]* [result_format_count u16] [result_formats u16]*`.
/// V1 REJECTS with `0A000`.
pub const FE_BIND: u8 = b'B';

/// Extended Query — Describe: `D [length] ['S'|'P'] [name\0]`. V1
/// REJECTS with `0A000`.
pub const FE_DESCRIBE: u8 = b'D';

/// Extended Query — Execute: `E [length] [portal\0] [max_rows u32]`.
/// V1 REJECTS with `0A000`.
pub const FE_EXECUTE: u8 = b'E';

/// Extended Query — Sync: `S [length=4]`. Resets error state. V1
/// REJECTS the whole extended-query subprotocol with `0A000`.
pub const FE_SYNC: u8 = b'S';

/// Extended Query — Close: `C [length] ['S'|'P'] [name\0]`. V1
/// REJECTS with `0A000`.
pub const FE_CLOSE: u8 = b'C';

/// Extended Query — Flush: `H [length=4]`. V1 REJECTS with `0A000`.
pub const FE_FLUSH: u8 = b'H';

/// COPY data from client: `d [length] [data]`. V1 REJECTS the whole
/// COPY subprotocol with `0A000`. V2 (T26) implements.
pub const FE_COPY_DATA: u8 = b'd';

/// COPY done from client: `c [length=4]`. V1 REJECTS.
pub const FE_COPY_DONE: u8 = b'c';

/// COPY fail from client: `f [length] [error\0]`. V1 REJECTS.
pub const FE_COPY_FAIL: u8 = b'f';

/// Function Call (deprecated since PG 8.0): `F [length] ...`. V1
/// REJECTS with `0A000`; never V2.
pub const FE_FUNCTION_CALL: u8 = b'F';

// ───────────────────────────────────────────────────────────────────────
// Backend (server → client) message-type tags (PG §55.2.2 / §55.7)
// ───────────────────────────────────────────────────────────────────────

/// Authentication request: `R [length] [auth_type u32 BE] [payload]`.
/// V1 emits AuthenticationSASL → SASLContinue → SASLFinal →
/// AuthenticationOk. Spec §3.3.
pub const BE_AUTHENTICATION: u8 = b'R';

/// ParameterStatus: `S [length] [key\0] [value\0]`. V1 emits one per
/// param after auth success: server_version, server_encoding,
/// client_encoding, DateStyle, TimeZone, integer_datetimes,
/// standard_conforming_strings, application_name. Spec §3 / §8.4.
pub const BE_PARAMETER_STATUS: u8 = b'S';

/// BackendKeyData: `K [length=12] [pid u32 BE] [secret u32 BE]`. V1
/// generates the pair via `kessel-crypto::sha256` but does NOT
/// action incoming CancelRequest (V2 feature). Spec §3 / §8.4 / §12.
pub const BE_BACKEND_KEY_DATA: u8 = b'K';

/// ReadyForQuery: `Z [length=5] [status u8: 'I' | 'T' | 'E']`. V1
/// always emits 'I' (idle) — V1 has no transaction-block state.
/// Spec §3 / §8.4.
pub const BE_READY_FOR_QUERY: u8 = b'Z';

/// RowDescription: `T [length] [field_count u16] [field_desc]*`. V1
/// emits per result set; field-format=0 (text) always. Spec §3 / §5.
pub const BE_ROW_DESCRIPTION: u8 = b'T';

/// DataRow: `D [length] [col_count u16] [col_length u32 BE | -1 for
/// NULL] [col_data]*`. V1 streams per row. Spec §3 / §5.
pub const BE_DATA_ROW: u8 = b'D';

/// CommandComplete: `C [length] [tag\0]`. Tag examples: "SELECT N",
/// "INSERT 0 N", "UPDATE N", "DELETE N", "SET", "CREATE TABLE".
/// Spec §3 / §10 T6.
pub const BE_COMMAND_COMPLETE: u8 = b'C';

/// ErrorResponse: `E [length] [field_type u8] [field_value\0]* \0`.
/// V1 emits S, V, C, M always; D / H / P emit when KesselDB provides
/// the detail. Spec §3 / §7.
pub const BE_ERROR_RESPONSE: u8 = b'E';

/// NoticeResponse: `N [length] ...` — same shape as ErrorResponse
/// but for warnings. V1 may emit during auth (e.g. server_version
/// translation note); otherwise reserved.
pub const BE_NOTICE_RESPONSE: u8 = b'N';

/// EmptyQueryResponse: `I [length=4]`. V1 emits when the parser sees
/// only whitespace/comments in a `Q` message. PG §55.2.2 §3.
pub const BE_EMPTY_QUERY_RESPONSE: u8 = b'I';

/// ParameterDescription: `t [length] [count u16] [type_oid u32 BE]*`.
/// V1 doesn't emit (only Extended Query path uses it). V2 (SP-PG-EXTQ).
pub const BE_PARAMETER_DESCRIPTION: u8 = b't';

/// ParseComplete: `1 [length=4]`. Extended Query reply. V1 doesn't
/// emit. V2.
pub const BE_PARSE_COMPLETE: u8 = b'1';

/// BindComplete: `2 [length=4]`. Extended Query reply. V1 doesn't
/// emit. V2.
pub const BE_BIND_COMPLETE: u8 = b'2';

/// CloseComplete: `3 [length=4]`. Extended Query reply to a `C`
/// Close message that successfully dropped a statement or portal.
/// V1 doesn't emit. V2 SP-PG-EXTQ T8 emits.
pub const BE_CLOSE_COMPLETE: u8 = b'3';

/// NoData: `n [length=4]`. Extended Query reply when a statement
/// returns no rows. V1 doesn't emit. V2.
pub const BE_NO_DATA: u8 = b'n';

/// PortalSuspended: `s [length=4]`. Extended Query reply when an
/// Execute hit its row limit. V1 doesn't emit. V2.
pub const BE_PORTAL_SUSPENDED: u8 = b's';

// ───────────────────────────────────────────────────────────────────────
// Authentication sub-codes (the u32 BE payload of an 'R' message)
// PG §55.2.6 / RFC 7677
// ───────────────────────────────────────────────────────────────────────

/// AuthenticationOk subcode. The server sends this after successful
/// auth — V1 emits at the end of the SCRAM exchange. Spec §3.3.
pub const AUTH_OK: u32 = 0;

/// AuthenticationCleartextPassword subcode. V1 never emits (SCRAM-
/// only); listed so the T2 SCRAM state machine can ignore stray
/// cleartext password client responses cleanly.
pub const AUTH_CLEARTEXT_PASSWORD: u32 = 3;

/// AuthenticationMD5Password subcode. V1 never emits (SCRAM-only;
/// MD5 is deprecated by PG 14+); listed for completeness.
pub const AUTH_MD5_PASSWORD: u32 = 5;

/// AuthenticationSASL subcode. V1 emits at the start of the auth
/// exchange — payload is a NUL-terminated mechanism name list ending
/// in another NUL (e.g. `"SCRAM-SHA-256\0\0"`). Spec §3.3.
pub const AUTH_SASL: u32 = 10;

/// AuthenticationSASLContinue subcode. V1 emits between client_first
/// and client_final — payload is the SCRAM `server_first` message.
/// Spec §3.3.
pub const AUTH_SASL_CONTINUE: u32 = 11;

/// AuthenticationSASLFinal subcode. V1 emits after client_final —
/// payload is the SCRAM `server_final` (server signature). Spec §3.3.
pub const AUTH_SASL_FINAL: u32 = 12;

// ───────────────────────────────────────────────────────────────────────
// ReadyForQuery transaction-status indicator (the single u8 payload)
// ───────────────────────────────────────────────────────────────────────

/// Idle — no transaction in progress. V1 always emits this (V1 has
/// no transaction-block state). PG §55.2.2 / §55.7.
pub const READY_FOR_QUERY_IDLE: u8 = b'I';

/// In a transaction block. V1 doesn't use (V1 dispatches via
/// `EngineApply::apply_sql` which is auto-commit). Future BEGIN /
/// COMMIT / ROLLBACK awareness would flip this.
pub const READY_FOR_QUERY_IN_TX: u8 = b'T';

/// In a failed transaction block. PG protocol requires `ROLLBACK`
/// to recover. V1 doesn't use.
pub const READY_FOR_QUERY_FAILED_TX: u8 = b'E';

// ───────────────────────────────────────────────────────────────────────
// PostgreSQL type OIDs that V1 emits in RowDescription (spec §5)
// PG `src/include/catalog/pg_type.dat`
// ───────────────────────────────────────────────────────────────────────

/// `bool` — wire text representation is `t` / `f` (NOT
/// `true`/`false`). KesselDB `FieldKind::Bool` maps here.
pub const PG_TYPE_BOOL: u32 = 16;

/// `bytea` — wire text representation is `\\x<hex>`. KesselDB
/// `FieldKind::Bytes` / `Ref` / `OverflowRef` map here.
pub const PG_TYPE_BYTEA: u32 = 17;

/// `int8` (i64) — KesselDB `FieldKind::I64` / `U32` / `U64` map here.
pub const PG_TYPE_INT8: u32 = 20;

/// `int2` (i16) — KesselDB `FieldKind::I8` / `I16` / `U8` map here.
pub const PG_TYPE_INT2: u32 = 21;

/// `int4` (i32) — KesselDB `FieldKind::I32` / `U16` maps here.
pub const PG_TYPE_INT4: u32 = 23;

/// `text` (varlena UTF-8) — KesselDB `FieldKind::Char` maps here.
pub const PG_TYPE_TEXT: u32 = 25;

/// `oid` (u32, 4-byte unsigned object identifier) — used by the
/// SP-PG-CAT synthesizers (pg_class.oid, pg_namespace.oid,
/// pg_attribute.attrelid, etc.) for the OID-shaped columns every
/// pg_catalog table carries. Locked vs `pg_type.dat`.
pub const PG_TYPE_OID: u32 = 26;

/// `float4` (f32) — not yet used by V1 (KesselDB has no f32
/// FieldKind); reserved for the eventual FLOAT4 / REAL FieldKind.
pub const PG_TYPE_FLOAT4: u32 = 700;

/// `float8` (f64) — not yet used by V1 (KesselDB has no f64
/// FieldKind); reserved.
pub const PG_TYPE_FLOAT8: u32 = 701;

/// `timestamptz` — KesselDB `FieldKind::Timestamp` (u64 nanos) maps
/// here. Wire text: `YYYY-MM-DD HH:MM:SS.ffffff+00`.
pub const PG_TYPE_TIMESTAMPTZ: u32 = 1184;

/// `numeric` (arbitrary precision decimal) — KesselDB `FieldKind::
/// U128` / `I128` / `Fixed` map here. V1 wire text only; V2 binary.
pub const PG_TYPE_NUMERIC: u32 = 1700;

/// `varchar` (varlena UTF-8 with length cap) — reserved for a
/// potential future KesselDB type; V1 prefers `text` (PG clients
/// accept both interchangeably).
pub const PG_TYPE_VARCHAR: u32 = 1043;

// ───────────────────────────────────────────────────────────────────────
// Format codes (per-column wire encoding)
// ───────────────────────────────────────────────────────────────────────

/// Text format — every column rendered as a UTF-8 byte sequence.
/// V1 emits this exclusively. Spec §5.1.
pub const FORMAT_CODE_TEXT: u16 = 0;

/// Binary format — type-specific big-endian bytes. V2 only.
pub const FORMAT_CODE_BINARY: u16 = 1;

// ───────────────────────────────────────────────────────────────────────
// Length-field rules (PG §55.2.1)
// ───────────────────────────────────────────────────────────────────────

/// Minimum valid length field value (length 4 = empty payload).
/// A length less than this is a protocol violation. Spec §3.1.
pub const PG_MIN_MESSAGE_LENGTH: u32 = 4;

/// Length of a NULL DataRow column (the column-length field is the
/// signed 32-bit value -1, encoded as `0xFFFFFFFF`). PG §55.2.2 §10.
/// V1 uses when the KesselDB row's null bitmap marks a field NULL.
pub const PG_DATA_ROW_COL_NULL_SENTINEL: i32 = -1;

#[cfg(test)]
mod tests {
    use super::*;

    // ───────────────────────────────────────────────────────────────────
    // T1 KATs — lock the spec invariants. Every constant in this module
    // is a wire-protocol byte / number that future code (T2..T18) will
    // depend on; flipping any value silently breaks every PG client on
    // earth. The KATs guard against that.
    //
    // Sources: PostgreSQL §55 (Frontend/Backend Protocol) for every
    // tag + numeric constant; RFC 5802 / RFC 7677 for SCRAM; RFC 8018
    // §5.2 for PBKDF2.
    // ───────────────────────────────────────────────────────────────────

    #[test]
    fn t1_pg_protocol_version_3_0_is_196608() {
        // PG §55.2.1: "The protocol version number. The most
        // significant 16 bits are the major version number (3 for
        // the protocol described here). The least significant 16
        // bits are the minor version number (0 for the protocol
        // described here)." 3 << 16 = 0x00030000 = 196608.
        assert_eq!(PG_PROTOCOL_VERSION_3_0, 196608);
        assert_eq!(PG_PROTOCOL_VERSION_3_0, 0x0003_0000);
        assert_eq!(PG_PROTOCOL_VERSION_3_0 >> 16, 3); // major
        assert_eq!(PG_PROTOCOL_VERSION_3_0 & 0xFFFF, 0); // minor
    }

    #[test]
    fn t1_pre_handshake_magic_codes_match_pg_postmaster_h() {
        // PG `src/include/libpq/pqcomm.h`:
        //   #define NEGOTIATE_SSL_CODE     PG_PROTOCOL(1234,5679)  → 80877103
        //   #define NEGOTIATE_GSS_CODE     PG_PROTOCOL(1234,5680)  → 80877104
        //   #define CANCEL_REQUEST_CODE    PG_PROTOCOL(1234,5678)  → 80877102
        // where PG_PROTOCOL(m,n) = (m << 16) | n.
        assert_eq!(PG_SSL_REQUEST_CODE, (1234u32 << 16) | 5679);
        assert_eq!(PG_GSS_ENC_REQUEST_CODE, (1234u32 << 16) | 5680);
        assert_eq!(PG_CANCEL_REQUEST_CODE, (1234u32 << 16) | 5678);
        assert_eq!(PG_SSL_REQUEST_CODE, 80877103);
        assert_eq!(PG_CANCEL_REQUEST_CODE, 80877102);
        assert_eq!(PG_GSS_ENC_REQUEST_CODE, 80877104);
    }

    #[test]
    fn t1_frontend_message_type_tags_match_pg_55_7_table() {
        // PG §55.7 — Frontend Messages. Every tag is one ASCII byte,
        // case-sensitive. These are the V1 + V2-rejected tags the
        // T2/T3 parsers will branch on.
        assert_eq!(FE_QUERY, b'Q');
        assert_eq!(FE_TERMINATE, b'X');
        assert_eq!(FE_PASSWORD, b'p');
        assert_eq!(FE_PARSE, b'P');
        assert_eq!(FE_BIND, b'B');
        assert_eq!(FE_DESCRIBE, b'D');
        assert_eq!(FE_EXECUTE, b'E');
        assert_eq!(FE_SYNC, b'S');
        assert_eq!(FE_CLOSE, b'C');
        assert_eq!(FE_FLUSH, b'H');
        assert_eq!(FE_COPY_DATA, b'd');
        assert_eq!(FE_COPY_DONE, b'c');
        assert_eq!(FE_COPY_FAIL, b'f');
        assert_eq!(FE_FUNCTION_CALL, b'F');
    }

    #[test]
    fn t1_backend_message_type_tags_match_pg_55_7_table() {
        // PG §55.7 — Backend Messages. T5/T6/T7 encoders emit these.
        // Note: 'D' and 'E' and 'S' and 'C' are REUSED across
        // frontend/backend with different semantics — the protocol
        // disambiguates by which side sent the byte (no in-band
        // direction flag).
        assert_eq!(BE_AUTHENTICATION, b'R');
        assert_eq!(BE_PARAMETER_STATUS, b'S');
        assert_eq!(BE_BACKEND_KEY_DATA, b'K');
        assert_eq!(BE_READY_FOR_QUERY, b'Z');
        assert_eq!(BE_ROW_DESCRIPTION, b'T');
        assert_eq!(BE_DATA_ROW, b'D');
        assert_eq!(BE_COMMAND_COMPLETE, b'C');
        assert_eq!(BE_ERROR_RESPONSE, b'E');
        assert_eq!(BE_NOTICE_RESPONSE, b'N');
        assert_eq!(BE_EMPTY_QUERY_RESPONSE, b'I');
        assert_eq!(BE_PARAMETER_DESCRIPTION, b't');
        assert_eq!(BE_PARSE_COMPLETE, b'1');
        assert_eq!(BE_BIND_COMPLETE, b'2');
        assert_eq!(BE_CLOSE_COMPLETE, b'3');
        assert_eq!(BE_NO_DATA, b'n');
        assert_eq!(BE_PORTAL_SUSPENDED, b's');
    }

    #[test]
    fn t1_authentication_subcodes_match_pg_55_7_authentication() {
        // PG §55.7 / "Authentication" message — the u32 BE that
        // follows the type byte + length is the auth-type code:
        //   0  AuthenticationOk
        //   3  AuthenticationCleartextPassword
        //   5  AuthenticationMD5Password
        //   10 AuthenticationSASL
        //   11 AuthenticationSASLContinue
        //   12 AuthenticationSASLFinal
        // T2's SCRAM state machine emits 10 → 11 → 12 → 0.
        assert_eq!(AUTH_OK, 0);
        assert_eq!(AUTH_CLEARTEXT_PASSWORD, 3);
        assert_eq!(AUTH_MD5_PASSWORD, 5);
        assert_eq!(AUTH_SASL, 10);
        assert_eq!(AUTH_SASL_CONTINUE, 11);
        assert_eq!(AUTH_SASL_FINAL, 12);
    }

    #[test]
    fn t1_ready_for_query_status_indicators_match_pg_55_2_2() {
        // PG §55.2.2 / ReadyForQuery: "indicates the current
        // backend transaction status: 'I' if idle (not in a
        // transaction block); 'T' if in a transaction block;
        // 'E' if in a failed transaction block." V1 always emits
        // 'I' (no transaction-block state in V1).
        assert_eq!(READY_FOR_QUERY_IDLE, b'I');
        assert_eq!(READY_FOR_QUERY_IN_TX, b'T');
        assert_eq!(READY_FOR_QUERY_FAILED_TX, b'E');
    }

    #[test]
    fn t1_pg_type_oids_match_pg_type_dat() {
        // PG `src/include/catalog/pg_type.dat` — the canonical type
        // OIDs every client library hard-codes. Spec §5 type table.
        // T4/T5 encoders depend on these byte-for-byte; flipping one
        // value silently corrupts every RowDescription on the wire.
        assert_eq!(PG_TYPE_BOOL, 16);
        assert_eq!(PG_TYPE_BYTEA, 17);
        assert_eq!(PG_TYPE_INT8, 20);
        assert_eq!(PG_TYPE_INT2, 21);
        assert_eq!(PG_TYPE_INT4, 23);
        assert_eq!(PG_TYPE_TEXT, 25);
        assert_eq!(PG_TYPE_FLOAT4, 700);
        assert_eq!(PG_TYPE_FLOAT8, 701);
        assert_eq!(PG_TYPE_VARCHAR, 1043);
        assert_eq!(PG_TYPE_TIMESTAMPTZ, 1184);
        assert_eq!(PG_TYPE_NUMERIC, 1700);
    }

    #[test]
    fn t1_format_codes_text_zero_binary_one_per_pg_55_2_2() {
        // PG §55.2.2 — Bind / RowDescription format codes: 0 = text,
        // 1 = binary. V1 emits text exclusively in RowDescription.
        assert_eq!(FORMAT_CODE_TEXT, 0);
        assert_eq!(FORMAT_CODE_BINARY, 1);
    }

    #[test]
    fn t1_framing_length_invariants_match_spec_3_1() {
        // Spec §3.1: "Length includes itself but NOT the type byte."
        // Minimum valid length = 4 (empty payload). NULL column
        // sentinel in DataRow is -1 as i32 (= 0xFFFFFFFF unsigned).
        assert_eq!(PG_MIN_MESSAGE_LENGTH, 4);
        assert_eq!(PG_DATA_ROW_COL_NULL_SENTINEL, -1);
        assert_eq!(PG_DATA_ROW_COL_NULL_SENTINEL as u32, 0xFFFFFFFFu32);
        // The signed -1 / unsigned 0xFFFFFFFF equivalence is the
        // RFC-derived NULL marker every PG client switches on.
    }
}
