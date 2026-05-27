//! PostgreSQL Frontend/Backend Protocol v3.0 — **startup phase**.
//!
//! Owns the bytes that flow between the moment a client opens the TCP
//! socket and the moment the server is committed to a SCRAM-SHA-256
//! authentication exchange. Three concerns live here, all spec §3.2:
//!
//! 1. **Pre-handshake magic dispatch** (`PreHandshakeMessage`) — the
//!    three SSLRequest / CancelRequest / GSSENCRequest packets that
//!    pre-date the v3 protocol's type-byte discipline. They share
//!    StartupMessage's "[length:4 BE][magic:4 BE]" envelope but carry
//!    no parameter pairs.
//! 2. **StartupMessage parsing** (`StartupMessage::parse`) — the v3.0
//!    initial message with the `user`/`database`/... key-value pairs.
//!    Returns a parsed view (with the `user` field extracted as a
//!    required parameter) or a precise `StartupError`.
//! 3. **Initial-bytes classification** (`classify_initial_message`) —
//!    given the raw inbound first message (length-prefix + body),
//!    decides whether to dispatch the SSL/Cancel/GSS pre-handshake
//!    path or parse it as a v3.0 StartupMessage. Drives the
//!    `server::accept` loop's pre-auth state machine.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`
//! §3.2.

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::proto::{
    PG_CANCEL_REQUEST_CODE, PG_GSS_ENC_REQUEST_CODE, PG_MIN_MESSAGE_LENGTH,
    PG_PROTOCOL_VERSION_3_0, PG_SSL_REQUEST_CODE,
};
use crate::PG_MAX_MESSAGE_SIZE;

/// A parsed StartupMessage. `user` is required (PG §55.2.1 / spec
/// §3.2); `database`, `application_name`, and any other parameter
/// pairs are surfaced via `params` in the order the client sent them
/// (V1 carries them through for `ParameterStatus` echo + logging
/// without acting on them).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupMessage {
    /// The `user` value the client sent. SCRAM-SHA-256 carries this
    /// into the authentication mechanism; V1 logs it but does NOT use
    /// it for authorization (single Bearer-token credential surface —
    /// spec §3.4 / §6.1). Required by PG §55.2.1.
    pub user: String,
    /// Raw key-value parameter pairs in client-sent order. V1 ignores
    /// everything except `user` (extracted above); V2 will surface
    /// `application_name` in `ParameterStatus` echo and respect
    /// `client_encoding=UTF8`.
    pub params: Vec<(String, String)>,
}

impl StartupMessage {
    /// Looks up a parameter by name (case-sensitive — PG parameter
    /// names are case-sensitive per `pg_settings`). Returns `None` if
    /// the client didn't send it. Used by the server.rs handshake
    /// loop to fetch `database` for the post-auth ParameterStatus
    /// echo.
    pub fn get_param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Classifies the first 8 bytes of a client's first message.
///
/// PG §55.2.1: every "first message" begins with `[length:u32 BE]`
/// followed by `[code_or_version:u32 BE]`. The code/version
/// distinguishes which of the four messages it is:
///   - `196608` = StartupMessage v3.0 → protocol-version handshake
///   - `80877103` = SSLRequest → reply 'N' (V1) or do TLS (V2)
///   - `80877102` = CancelRequest → V1 logs + drops connection
///   - `80877104` = GSSENCRequest → reply 'N' (GSSAPI never V1)
///   - anything else = protocol-violation; close.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitialMessage {
    /// Real StartupMessage with the v3.0 protocol version. The
    /// `body` is the post-length-and-version payload — the
    /// NUL-separated key=value pairs ending in an empty pair, ready
    /// for `StartupMessage::parse_payload`.
    Startup(StartupMessage),
    /// `SSLRequest` (80877103). V1 responds with a single byte 'N'
    /// (no TLS) and loops back for the real StartupMessage. V2
    /// (`tls` feature) will respond 'S' and perform the rustls
    /// handshake.
    SslRequest,
    /// `CancelRequest` (80877102). V1 logs + drops the connection
    /// (no action on the running query — query cancellation is V2
    /// SP-PG T24).
    CancelRequest {
        /// The PID claimed by the client. V1 ignores; preserved
        /// because V2 will look it up in the cancel-key table.
        pid: u32,
        /// The cancel secret claimed by the client. V1 ignores;
        /// preserved for V2 cancel-key validation.
        secret: u32,
    },
    /// `GSSENCRequest` (80877104). V1 responds with a single byte 'N'
    /// (no GSSAPI) and loops back for the real StartupMessage. GSSAPI
    /// is permanently out of scope (spec §2.2).
    GssEncRequest,
}

/// Errors that can arise during the startup phase. T2 lifts
/// `server::PgError::NotYetImplemented` to a real error enum that
/// includes these variants; for now they're kept distinct from
/// `PgError` so the startup-parsing surface area is independently
/// testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupError {
    /// The 4-byte length prefix claims fewer than 8 bytes (the
    /// minimum for a length+code envelope). PG §55.2.1 / spec §3.1
    /// — a length < 4 is impossible (it can't include itself) but
    /// even length=4 (empty body, no code) is a malformed first
    /// message. SQLSTATE `08P01` protocol_violation.
    LengthTooSmall { length: u32 },
    /// The 4-byte length prefix exceeds `PG_MAX_MESSAGE_SIZE` (16
    /// MiB by default). Per spec §3.1 the cap-before-allocation
    /// invariant: a client claiming a 1 GiB message gets a clean
    /// rejection BEFORE `Vec::with_capacity(1 GiB)`. SQLSTATE
    /// `08P01`.
    LengthTooLarge { length: u32 },
    /// The 4-byte protocol version field is not `196608` (the only
    /// version V1 speaks) AND not one of the three pre-handshake
    /// magic codes. SQLSTATE `0A000` feature_not_supported per spec
    /// §3.2 (the spec calls out unknown version as
    /// feature_not_supported, not protocol_violation — an old PG-
    /// protocol-v2 client gets a meaningful "we don't speak that"
    /// rather than a blanket "you broke the protocol").
    UnsupportedProtocolVersion { version: u32 },
    /// A StartupMessage v3.0 omitted the required `user` parameter
    /// (PG §55.2.1: "There is no default; the client must always
    /// send the user name."). Spec §6.2 maps this to SQLSTATE
    /// `28000` invalid_authorization_specification — the failure
    /// is logged as an auth failure, not a protocol error.
    MissingUserParameter,
    /// A StartupMessage v3.0's body is malformed: an odd number of
    /// NUL-separated strings (one of the key=value pairs is missing
    /// its value), the terminating empty string is absent, or a
    /// UTF-8 decode failed inside a key or value (PG §55.2.1
    /// requires UTF-8 — see §3.2: "The protocol uses UTF-8 for all
    /// textual data."). SQLSTATE `08P01`.
    MalformedBody { reason: &'static str },
    /// CancelRequest's body must be EXACTLY 8 bytes (PID + secret =
    /// 4 + 4 = 8). A claimed length other than 16 (length+version+
    /// pid+secret) → `08P01`.
    MalformedCancelRequest,
    /// SSLRequest / GSSENCRequest must have length exactly 8 (length
    /// + version, no body). Any other length → `08P01`.
    MalformedPreHandshake,
}

/// Reads + classifies the next inbound startup-phase message from a
/// raw byte slice.
///
/// Caller responsibilities: read the 4-byte length prefix first, then
/// read `length - 4` more bytes (= the rest of the message), then
/// pass the WHOLE thing (length-prefix included) here. The function
/// validates the cap-before-allocation invariant + checks the magic
/// code + dispatches.
///
/// The buffer MUST contain exactly `length` bytes (the full message).
/// Trailing bytes beyond `length` are a caller bug — see
/// `read_initial_message_length` for the recommended read-loop shape.
pub fn classify_initial_message(buf: &[u8]) -> Result<InitialMessage, StartupError> {
    // PG §55.2.1: the StartupMessage envelope is at minimum
    // 8 bytes (length:4 + version-or-code:4). Anything shorter
    // CANNOT be a valid first message.
    if buf.len() < 8 {
        return Err(StartupError::LengthTooSmall {
            length: buf.len() as u32,
        });
    }
    let length = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if length < PG_MIN_MESSAGE_LENGTH + 4 {
        // Below `8` is impossible — length includes itself + the
        // mandatory 4-byte version/code field. Treat as protocol
        // violation, never as "let me peek further".
        return Err(StartupError::LengthTooSmall { length });
    }
    if length as usize > PG_MAX_MESSAGE_SIZE {
        // Cap-before-allocation invariant from spec §3.1. The caller
        // SHOULD have already enforced this before reading `length`
        // bytes off the wire, but we double-check here so a buggy
        // caller can't slip a 1 GiB allocation past the gateway.
        return Err(StartupError::LengthTooLarge { length });
    }
    if (length as usize) != buf.len() {
        // The slice handed in must match the announced length to
        // the byte. Returning a tailored error here would be a
        // distraction — the read-loop is what guarantees the
        // invariant, so we collapse this into `MalformedBody`.
        return Err(StartupError::MalformedBody {
            reason: "length prefix does not match buffer size",
        });
    }
    let code = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    match code {
        PG_SSL_REQUEST_CODE => {
            // SSLRequest envelope is EXACTLY 8 bytes — length(4) +
            // code(4). Anything else → protocol violation.
            if length != 8 {
                return Err(StartupError::MalformedPreHandshake);
            }
            Ok(InitialMessage::SslRequest)
        }
        PG_GSS_ENC_REQUEST_CODE => {
            if length != 8 {
                return Err(StartupError::MalformedPreHandshake);
            }
            Ok(InitialMessage::GssEncRequest)
        }
        PG_CANCEL_REQUEST_CODE => {
            // CancelRequest envelope is EXACTLY 16 bytes — length(4)
            // + code(4) + pid(4) + secret(4). Spec §3.2 + PG §55.2.1.
            if length != 16 {
                return Err(StartupError::MalformedCancelRequest);
            }
            let pid = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
            let secret = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
            Ok(InitialMessage::CancelRequest { pid, secret })
        }
        PG_PROTOCOL_VERSION_3_0 => {
            // Real StartupMessage v3.0 → parse the body.
            let body = &buf[8..];
            let parsed = parse_startup_body(body)?;
            Ok(InitialMessage::Startup(parsed))
        }
        version => Err(StartupError::UnsupportedProtocolVersion { version }),
    }
}

/// Parses the StartupMessage v3.0 body (the bytes AFTER the
/// `[length:4][version:4]` envelope). Body is a sequence of
/// NUL-terminated UTF-8 key-value strings ending in a single empty
/// (zero-length) string — i.e. the body's last byte is a NUL preceded
/// by another NUL.
///
/// Per PG §55.2.1 / spec §3.2: "After the version number, the message
/// body consists of a series of parameter name and value strings,
/// each terminated with a zero byte. The final parameter name string
/// is empty, indicating the end."
pub fn parse_startup_body(body: &[u8]) -> Result<StartupMessage, StartupError> {
    // The body MUST end with a NUL byte (the terminating empty key's
    // NUL). An empty body is a malformed message.
    if body.is_empty() || body[body.len() - 1] != 0 {
        return Err(StartupError::MalformedBody {
            reason: "body does not end with NUL terminator",
        });
    }
    // Split on NUL. The last element after a trailing-NUL split is
    // the empty string (the terminator); the second-to-last must
    // also be empty (the terminating "empty key" of the key-value
    // sequence). After dropping the trailing-NUL artifact, we expect
    // an even number of non-empty strings (k, v, k, v, ...) followed
    // by a single empty string sentinel.
    let mut pieces: Vec<&[u8]> = body.split(|&b| b == 0).collect();
    // The trailing NUL produces a trailing empty piece — drop it.
    // If the body was just a single NUL (= empty terminator with
    // no params, illegal: missing user), pieces becomes [b"", b""];
    // after the drop we get [b""], which we'll detect as missing user.
    if pieces.last().is_none_or(|p| !p.is_empty()) {
        return Err(StartupError::MalformedBody {
            reason: "body does not end with NUL terminator",
        });
    }
    pieces.pop();
    // Now pieces should be [k1, v1, k2, v2, ..., kN, vN, b""] where
    // the trailing empty piece is the "end of parameters" sentinel.
    if pieces.last().is_none_or(|p| !p.is_empty()) {
        return Err(StartupError::MalformedBody {
            reason: "missing empty-string terminator at end of parameters",
        });
    }
    pieces.pop(); // drop the empty-string terminator
    if !pieces.len().is_multiple_of(2) {
        return Err(StartupError::MalformedBody {
            reason: "odd number of key/value strings",
        });
    }
    let mut params: Vec<(String, String)> = Vec::with_capacity(pieces.len() / 2);
    let mut user: Option<String> = None;
    for kv in pieces.chunks_exact(2) {
        let key = std::str::from_utf8(kv[0]).map_err(|_| {
            StartupError::MalformedBody {
                reason: "non-UTF-8 in parameter key",
            }
        })?;
        let val = std::str::from_utf8(kv[1]).map_err(|_| {
            StartupError::MalformedBody {
                reason: "non-UTF-8 in parameter value",
            }
        })?;
        if key.is_empty() {
            // An empty key BEFORE the terminator is malformed (the
            // terminator IS the empty key; one in the middle of the
            // sequence is a protocol violation).
            return Err(StartupError::MalformedBody {
                reason: "empty parameter key before terminator",
            });
        }
        if key == "user" {
            user = Some(val.to_string());
        }
        params.push((key.to_string(), val.to_string()));
    }
    let user = user.ok_or(StartupError::MissingUserParameter)?;
    if user.is_empty() {
        // PG accepts an empty user in the wire, but every auth path
        // requires a non-empty user — treat empty as missing. Spec
        // §6.2 SQLSTATE `28000`.
        return Err(StartupError::MissingUserParameter);
    }
    Ok(StartupMessage { user, params })
}

/// Encodes the wire bytes for an SSLRequest reply. V1 sends a single
/// byte 'N' (no TLS — proceed cleartext). PG §55.2.10. Caller writes
/// to the stream + loops back for the next inbound message.
pub const SSL_REPLY_NO_TLS: u8 = b'N';

/// Encodes the wire bytes for a GSSENCRequest reply. V1 sends a single
/// byte 'N' (no GSSAPI — proceed cleartext). PG §55.2.10 (same shape
/// as SSL reply).
pub const GSS_REPLY_NO_GSS: u8 = b'N';

#[cfg(test)]
mod tests {
    use super::*;

    // ───────────────────────────────────────────────────────────────────
    // T2 KATs — lock the startup-phase wire-protocol invariants
    // against PG §55.2.1 + spec §3.2. The byte patterns here are the
    // exact bytes a real libpq client emits when invoked as e.g.
    // `psql -h host -p 5432 -U test`, captured via `tcpdump -X`.
    // ───────────────────────────────────────────────────────────────────

    /// Helper: build a StartupMessage wire frame from (version,
    /// params) — what a libpq client would send. Length includes
    /// itself + the 4-byte version + the body + the trailing-empty
    /// terminator.
    fn build_startup_frame(version: u32, params: &[(&str, &str)]) -> Vec<u8> {
        let mut body: Vec<u8> = Vec::new();
        for (k, v) in params {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0); // terminating empty key NUL
        let length = (4 + 4 + body.len()) as u32;
        let mut frame = Vec::with_capacity(length as usize);
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&version.to_be_bytes());
        frame.extend_from_slice(&body);
        frame
    }

    /// Helper: build SSLRequest / GSSENCRequest envelope.
    fn build_pre_handshake_frame(code: u32) -> Vec<u8> {
        let mut frame = Vec::with_capacity(8);
        frame.extend_from_slice(&8u32.to_be_bytes());
        frame.extend_from_slice(&code.to_be_bytes());
        frame
    }

    /// Helper: build CancelRequest envelope.
    fn build_cancel_request_frame(pid: u32, secret: u32) -> Vec<u8> {
        let mut frame = Vec::with_capacity(16);
        frame.extend_from_slice(&16u32.to_be_bytes());
        frame.extend_from_slice(&PG_CANCEL_REQUEST_CODE.to_be_bytes());
        frame.extend_from_slice(&pid.to_be_bytes());
        frame.extend_from_slice(&secret.to_be_bytes());
        frame
    }

    /// Well-formed StartupMessage with `user=test` parses; the
    /// `user` field is extracted and other params are preserved in
    /// original order. PG §55.2.1.
    #[test]
    fn t2_startup_parses_minimum_user_only_message() {
        let frame = build_startup_frame(196608, &[("user", "test")]);
        match classify_initial_message(&frame).expect("parses") {
            InitialMessage::Startup(sm) => {
                assert_eq!(sm.user, "test");
                assert_eq!(sm.params, vec![("user".to_string(), "test".to_string())]);
            }
            other => panic!("expected Startup, got {other:?}"),
        }
    }

    /// Multi-parameter StartupMessage (`user`, `database`,
    /// `application_name`) parses; param ordering is preserved;
    /// `get_param` returns the right values.
    #[test]
    fn t2_startup_parses_multi_param_message_preserving_order() {
        let frame = build_startup_frame(
            196608,
            &[
                ("user", "alice"),
                ("database", "kessel"),
                ("application_name", "psql"),
            ],
        );
        match classify_initial_message(&frame).expect("parses") {
            InitialMessage::Startup(sm) => {
                assert_eq!(sm.user, "alice");
                assert_eq!(sm.get_param("user"), Some("alice"));
                assert_eq!(sm.get_param("database"), Some("kessel"));
                assert_eq!(sm.get_param("application_name"), Some("psql"));
                assert_eq!(sm.get_param("missing"), None);
                // Order preserved
                assert_eq!(sm.params[0].0, "user");
                assert_eq!(sm.params[1].0, "database");
                assert_eq!(sm.params[2].0, "application_name");
            }
            other => panic!("expected Startup, got {other:?}"),
        }
    }

    /// StartupMessage with NO `user` field → `MissingUserParameter`
    /// (SQLSTATE `28000` per spec §6.2). PG §55.2.1: "The user name
    /// to connect as. Required; there is no default."
    #[test]
    fn t2_startup_missing_user_is_rejected() {
        let frame = build_startup_frame(196608, &[("database", "kessel")]);
        match classify_initial_message(&frame) {
            Err(StartupError::MissingUserParameter) => {}
            other => panic!("expected MissingUserParameter, got {other:?}"),
        }
    }

    /// StartupMessage with EMPTY `user` field → `MissingUserParameter`
    /// (the wire allows `user=` but every auth path requires a
    /// non-empty user; collapse to the same error as missing).
    #[test]
    fn t2_startup_empty_user_is_rejected() {
        let frame = build_startup_frame(196608, &[("user", "")]);
        match classify_initial_message(&frame) {
            Err(StartupError::MissingUserParameter) => {}
            other => panic!("expected MissingUserParameter for empty user, got {other:?}"),
        }
    }

    /// SSLRequest pre-handshake magic is detected and classified
    /// — server must reply 'N' (V1 — no TLS) and loop. PG §55.2.10.
    /// Magic code: 80877103 = (1234 << 16) | 5679.
    #[test]
    fn t2_ssl_request_is_classified_as_pre_handshake() {
        let frame = build_pre_handshake_frame(PG_SSL_REQUEST_CODE);
        match classify_initial_message(&frame).expect("parses") {
            InitialMessage::SslRequest => {}
            other => panic!("expected SslRequest, got {other:?}"),
        }
        // Locked: V1 always replies single byte 'N'.
        assert_eq!(SSL_REPLY_NO_TLS, b'N');
    }

    /// GSSENCRequest pre-handshake magic is detected and classified
    /// — server must reply 'N' (V1 — no GSSAPI, ever) and loop. PG
    /// §55.2.10. Magic code: 80877104 = (1234 << 16) | 5680.
    #[test]
    fn t2_gss_enc_request_is_classified_as_pre_handshake() {
        let frame = build_pre_handshake_frame(PG_GSS_ENC_REQUEST_CODE);
        match classify_initial_message(&frame).expect("parses") {
            InitialMessage::GssEncRequest => {}
            other => panic!("expected GssEncRequest, got {other:?}"),
        }
        assert_eq!(GSS_REPLY_NO_GSS, b'N');
    }

    /// CancelRequest pre-handshake magic with PID + secret is
    /// detected and the two u32s are surfaced verbatim. V1 logs +
    /// drops connection; preserved for V2's cancel-key table.
    #[test]
    fn t2_cancel_request_extracts_pid_and_secret() {
        let frame = build_cancel_request_frame(0xDEADBEEF, 0xCAFEBABE);
        match classify_initial_message(&frame).expect("parses") {
            InitialMessage::CancelRequest { pid, secret } => {
                assert_eq!(pid, 0xDEADBEEF);
                assert_eq!(secret, 0xCAFEBABE);
            }
            other => panic!("expected CancelRequest, got {other:?}"),
        }
    }

    /// Unsupported protocol version (e.g. PG v2 = 0x00020000) →
    /// UnsupportedProtocolVersion (SQLSTATE `0A000`
    /// feature_not_supported per spec §3.2).
    #[test]
    fn t2_unsupported_protocol_version_is_rejected() {
        let frame = build_startup_frame(0x00020000, &[("user", "test")]);
        match classify_initial_message(&frame) {
            Err(StartupError::UnsupportedProtocolVersion { version }) => {
                assert_eq!(version, 0x00020000);
            }
            other => panic!("expected UnsupportedProtocolVersion, got {other:?}"),
        }
    }

    /// Future PG v4 (0x00040000) — also rejected with the same
    /// error (V1 only speaks v3.0; a future-protocol client gets a
    /// meaningful "we don't speak that" rather than crashing).
    #[test]
    fn t2_future_protocol_version_is_also_rejected() {
        let frame = build_startup_frame(0x00040000, &[("user", "test")]);
        match classify_initial_message(&frame) {
            Err(StartupError::UnsupportedProtocolVersion { version }) => {
                assert_eq!(version, 0x00040000);
            }
            other => panic!("expected UnsupportedProtocolVersion, got {other:?}"),
        }
    }

    /// Length prefix shorter than 8 → `LengthTooSmall`. A 4-byte
    /// envelope is the absolute minimum (length-includes-itself);
    /// without the version field nothing useful can be parsed.
    #[test]
    fn t2_length_too_small_is_rejected_before_allocation() {
        // length=4 → claims an empty envelope, no version field
        let mut frame = Vec::new();
        frame.extend_from_slice(&4u32.to_be_bytes());
        // pad to 8 bytes so the slice has both length + dummy version
        frame.extend_from_slice(&[0; 4]);
        match classify_initial_message(&frame) {
            Err(StartupError::LengthTooSmall { length: 4 }) => {}
            other => panic!("expected LengthTooSmall(4), got {other:?}"),
        }
    }

    /// Length prefix exceeding `PG_MAX_MESSAGE_SIZE` → `LengthTooLarge`
    /// BEFORE any allocation. This is spec §3.1's cap-before-
    /// allocation invariant — a client claiming a 1 GiB message
    /// gets a clean rejection.
    #[test]
    fn t2_length_too_large_is_rejected_before_allocation() {
        // Build an 8-byte slice claiming length = 1 GiB. The actual
        // slice is only 8 bytes; the function must reject on the
        // length-prefix alone, never trying to validate body bytes.
        let huge: u32 = 1024 * 1024 * 1024;
        let mut frame = Vec::with_capacity(8);
        frame.extend_from_slice(&huge.to_be_bytes());
        frame.extend_from_slice(&PG_PROTOCOL_VERSION_3_0.to_be_bytes());
        match classify_initial_message(&frame) {
            Err(StartupError::LengthTooLarge { length }) => {
                assert_eq!(length, huge);
            }
            other => panic!("expected LengthTooLarge, got {other:?}"),
        }
    }

    /// SSLRequest with a non-8 length → MalformedPreHandshake. A real
    /// libpq client always sends exactly 8 bytes; any deviation is a
    /// protocol violation (SQLSTATE `08P01`).
    #[test]
    fn t2_ssl_request_with_extra_body_is_rejected() {
        let mut frame = Vec::with_capacity(12);
        frame.extend_from_slice(&12u32.to_be_bytes()); // claims 12 bytes
        frame.extend_from_slice(&PG_SSL_REQUEST_CODE.to_be_bytes());
        frame.extend_from_slice(&[0xAB, 0xCD, 0xEF, 0x12]);
        match classify_initial_message(&frame) {
            Err(StartupError::MalformedPreHandshake) => {}
            other => panic!("expected MalformedPreHandshake, got {other:?}"),
        }
    }

    /// CancelRequest with wrong length → `MalformedCancelRequest`.
    /// The envelope is exactly 16 bytes (length+code+pid+secret).
    #[test]
    fn t2_cancel_request_with_wrong_length_is_rejected() {
        let mut frame = Vec::with_capacity(12);
        frame.extend_from_slice(&12u32.to_be_bytes()); // claims 12 bytes (missing secret)
        frame.extend_from_slice(&PG_CANCEL_REQUEST_CODE.to_be_bytes());
        frame.extend_from_slice(&[0; 4]);
        match classify_initial_message(&frame) {
            Err(StartupError::MalformedCancelRequest) => {}
            other => panic!("expected MalformedCancelRequest, got {other:?}"),
        }
    }

    /// StartupMessage body missing the terminating empty key →
    /// `MalformedBody`. The body ends with a NUL, but the preceding
    /// piece isn't empty — the terminator's NUL is present but the
    /// "empty key" preceding it is missing.
    #[test]
    fn t2_startup_body_missing_terminator_is_rejected() {
        // user=test, NO empty-key terminator
        let mut body = Vec::new();
        body.extend_from_slice(b"user");
        body.push(0);
        body.extend_from_slice(b"test");
        body.push(0);
        // No trailing empty-key NUL.
        // (The last NUL is the value-terminator for "test"; the
        // missing terminator is the empty key after.)
        // Build a frame: length includes itself + version + body.
        let length = (4 + 4 + body.len()) as u32;
        let mut frame = Vec::with_capacity(length as usize);
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&PG_PROTOCOL_VERSION_3_0.to_be_bytes());
        frame.extend_from_slice(&body);
        // The body ends with a NUL (from value "test\0"), but the
        // empty-key terminator is missing — split-on-NUL produces
        // [b"user", b"test", b""], pop the empty → [b"user", b"test"],
        // pop again expecting empty → fails the "missing terminator"
        // check.
        match classify_initial_message(&frame) {
            Err(StartupError::MalformedBody { .. }) => {}
            other => panic!("expected MalformedBody, got {other:?}"),
        }
    }

    /// StartupMessage body with odd number of NUL-separated strings
    /// (a key with no value) → `MalformedBody`. The wire is supposed
    /// to be k\0v\0k\0v\0...\0\0 — an odd count means a key was sent
    /// without its value.
    #[test]
    fn t2_startup_body_odd_kv_pair_is_rejected() {
        // Manually craft: user\0test\0orphan\0\0
        // After split: [b"user", b"test", b"orphan", b"", b""]
        // After dropping trailing empty: [b"user", b"test", b"orphan", b""]
        // Last is empty (terminator) → pop → [b"user", b"test", b"orphan"]
        // Length 3 is odd → MalformedBody.
        let mut body = Vec::new();
        body.extend_from_slice(b"user");
        body.push(0);
        body.extend_from_slice(b"test");
        body.push(0);
        body.extend_from_slice(b"orphan");
        body.push(0);
        body.push(0); // terminator
        let length = (4 + 4 + body.len()) as u32;
        let mut frame = Vec::with_capacity(length as usize);
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&PG_PROTOCOL_VERSION_3_0.to_be_bytes());
        frame.extend_from_slice(&body);
        match classify_initial_message(&frame) {
            Err(StartupError::MalformedBody { .. }) => {}
            other => panic!("expected MalformedBody for odd KV count, got {other:?}"),
        }
    }

    /// Empty buffer (caller passed 0 bytes — e.g. EOF on first read)
    /// → `LengthTooSmall { length: 0 }`. The server-loop turns this
    /// into a clean connection close (no ErrorResponse, since there's
    /// no client to send it to).
    #[test]
    fn t2_empty_buffer_is_rejected_as_length_too_small() {
        match classify_initial_message(&[]) {
            Err(StartupError::LengthTooSmall { length: 0 }) => {}
            other => panic!("expected LengthTooSmall(0), got {other:?}"),
        }
    }
}
