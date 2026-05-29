//! PG-gateway listener + per-connection accept loop.
//!
//! **T2 status (this commit):** real startup-handshake + SCRAM-SHA-256
//! auth + post-auth greeting (ParameterStatus + BackendKeyData +
//! ReadyForQuery). `accept` returns `Ok(AcceptedSession)` after the
//! client passes SCRAM and the greeting is sent; T3 will use the
//! returned session to enter the Simple Query loop.
//!
//! Wire shape (spec §3.2 + §3.3 + §3.4 + §6):
//!
//! ```text
//! TCP accept
//!   ↓
//! read first message (length-prefix → body)
//!   ↓
//! pre-handshake?
//!   ├─ SSLRequest      → write 'N' (no TLS), read next message
//!   ├─ GSSENCRequest   → write 'N' (no GSSAPI), read next message
//!   └─ CancelRequest   → drop connection (V1 takes no action)
//!   ↓
//! StartupMessage v3.0?
//!   ↓ (extract `user`, ignore others)
//! write AuthenticationSASL ("SCRAM-SHA-256\0\0")
//!   ↓
//! read SASLInitialResponse (p-tag, mech + client-first)
//!   ↓ (validate mech; parse client-first)
//! write AuthenticationSASLContinue (server-first)
//!   ↓
//! read SASLResponse (p-tag, client-final)
//!   ↓ (validate channel binding + nonce + proof)
//! write AuthenticationSASLFinal (server-signature)
//!   ↓
//! write AuthenticationOk
//!   ↓
//! write ParameterStatus * N (server_version, server_encoding, ...)
//!   ↓
//! write BackendKeyData (pid + secret)
//!   ↓
//! write ReadyForQuery ('I' = idle)
//!   ↓
//! return Ok(AcceptedSession { user })
//! ```
//!
//! T1 regression-lock — `t1_accept_returns_not_yet_implemented_stub` — is
//! flipped to T2 acceptance tests that drive the WHOLE handshake against
//! a fixed-nonce SCRAM client emulator and assert the post-auth
//! byte-sequence (AuthenticationOk + ParameterStatus + BackendKeyData
//! + ReadyForQuery) is correct.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::auth::{
    self, encode_authentication_ok, encode_authentication_sasl_challenge,
    encode_authentication_sasl_continue, encode_authentication_sasl_final, AuthError,
};
use crate::proto::{
    BE_BACKEND_KEY_DATA, BE_PARAMETER_STATUS, BE_READY_FOR_QUERY, FE_PASSWORD,
    READY_FOR_QUERY_IDLE,
};
use crate::startup::{
    classify_initial_message, InitialMessage, StartupError, GSS_REPLY_NO_GSS, SSL_REPLY_NO_TLS,
};
use crate::{PG_DEFAULT_SCRAM_ITERATIONS, PG_MAX_MESSAGE_SIZE};
use std::io::{Read, Write};

/// Errors a PG-wire session can return at any phase. T2 widens the
/// T1 placeholder enum into the real auth/protocol/io variants.
#[derive(Debug)]
pub enum PgError {
    /// Pre-auth protocol violation (StartupMessage parse failure,
    /// length-cap violation, unsupported protocol version). Spec
    /// §3.2 / §6.2 SQLSTATE mapping happens at the server-loop
    /// boundary; the variant carries the precise `StartupError` so
    /// the caller can render the right ErrorResponse.
    StartupFailed(StartupError),
    /// SCRAM-SHA-256 authentication failure (bad proof, nonce
    /// mismatch, malformed payload, etc.). Spec §6.2 — server sends
    /// ErrorResponse `28P01` invalid_password + closes TCP. No
    /// oracle for credential probing.
    AuthFailed(AuthError),
    /// `ServerConfig.token` is unset and `allow_anonymous` is false
    /// (spec §3.4). The accept loop closes the connection with
    /// `28000` invalid_authorization_specification BEFORE entering
    /// SCRAM.
    NoTokenConfigured,
    /// I/O error reading or writing the TCP stream — propagates the
    /// `std::io::ErrorKind` so the server loop can distinguish EOF
    /// (clean close) from connection-reset (client crashed).
    Io(std::io::ErrorKind),
    /// Inbound frame's length-prefix violated `PG_MAX_MESSAGE_SIZE`.
    /// Spec §3.1's cap-before-allocation invariant — a client claiming
    /// 1 GiB never reaches `Vec::with_capacity`. SQLSTATE `08P01`.
    MessageTooLarge { length: u32 },
    /// Expected a `p`-tag SASL response frame but the client sent a
    /// different message type during the auth phase. SQLSTATE `08P01`
    /// protocol_violation.
    UnexpectedMessageDuringAuth { tag: u8 },
    /// T16 (spec §9.3): the per-connection idle-read timeout fired
    /// before the client sent its next message. `run_session` emits
    /// a FATAL `57014` query_canceled ErrorResponse on the wire
    /// immediately BEFORE returning this variant — the caller (the
    /// `serve_pg` accept loop in `kesseldb-server`) just drops the
    /// thread. Distinguished from `Io(UnexpectedEof)` (peer-clean-
    /// close) and `Io(ConnectionReset)` (peer-RST) so the listener
    /// can log + count idle terminations separately if it wants to.
    IdleTimeout,
}

/// T16: classify the `std::io::ErrorKind` from a per-connection
/// `read_exact` against the timeout the caller installed via
/// `TcpStream::set_read_timeout(Some(pg_idle_timeout))`. On Linux
/// the timeout surfaces as `WouldBlock`; on Windows it surfaces as
/// `TimedOut`; the platform difference is locked in `std::io`'s
/// `TcpStream::set_read_timeout` documentation. We accept either so
/// the same code path fires across platforms.
///
/// Sibling helper to `kessel_http_gateway::ws::session::is_read_timeout`
/// — same shape, separate copy so neither crate depends on the other.
pub(crate) fn is_idle_timeout(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

impl From<StartupError> for PgError {
    fn from(e: StartupError) -> Self { PgError::StartupFailed(e) }
}

impl From<AuthError> for PgError {
    fn from(e: AuthError) -> Self { PgError::AuthFailed(e) }
}

impl From<std::io::Error> for PgError {
    fn from(e: std::io::Error) -> Self { PgError::Io(e.kind()) }
}

/// Outcome of a successful `accept` call. T3+ will use the
/// `user`/`pid`/`secret` fields to enter the Simple Query loop and
/// to wire BackendKeyData/CancelRequest pairing. T2 just constructs
/// it and returns.
#[derive(Debug, Clone)]
pub struct AcceptedSession {
    /// Username from the client's StartupMessage. V1 logs but
    /// doesn't use for authorization (spec §3.4 — one credential
    /// surface; SCRAM happens against the Bearer token).
    pub user: String,
    /// PID we sent in BackendKeyData. V1 deterministic-from-nonce
    /// per spec §3.4 (open question #4); preserved here for the
    /// post-T2 cancel-key table.
    pub pid: u32,
    /// Cancel secret we sent in BackendKeyData. Same notes as `pid`.
    pub secret: u32,
}

/// Reads ONE inbound message frame from the stream. Generic over
/// `tag_present`: pre-auth/StartupMessage frames have NO type tag
/// (only `[length:4][body]`); post-StartupMessage frames have a
/// 1-byte type tag prefix (`[tag:1][length:4][body]`).
///
/// Returns `(tag_or_zero, full_frame_bytes)` — for tagless frames
/// `tag_or_zero` is 0 and `full_frame_bytes` includes the length
/// prefix. For tagged frames the tag is returned separately and
/// `full_frame_bytes` starts at the length prefix.
fn read_message<R: Read>(
    r: &mut R,
    tag_present: bool,
) -> Result<(u8, Vec<u8>), PgError> {
    let tag = if tag_present {
        let mut t = [0u8; 1];
        r.read_exact(&mut t)?;
        t[0]
    } else {
        0
    };
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let length = u32::from_be_bytes(len_buf);
    // Cap-before-allocation invariant (spec §3.1).
    if length as usize > PG_MAX_MESSAGE_SIZE {
        return Err(PgError::MessageTooLarge { length });
    }
    if length < 4 {
        return Err(PgError::StartupFailed(StartupError::LengthTooSmall { length }));
    }
    // Body is `length - 4` bytes (length-includes-itself).
    let body_len = (length as usize) - 4;
    let mut frame = Vec::with_capacity(length as usize);
    frame.extend_from_slice(&len_buf);
    let mut body = vec![0u8; body_len];
    r.read_exact(&mut body)?;
    frame.extend_from_slice(&body);
    Ok((tag, frame))
}

/// Per-connection accept entry point. Drives the full startup +
/// SCRAM-SHA-256 + post-auth-greeting handshake against the stream.
///
/// Generic over `Read + Write` so tests can drive it with an
/// in-memory pair; production callers wire it to a `TcpStream` (the
/// `Cursor`-based shim makes that trivial).
///
/// **Operator contract (spec §3.4):** `token` MUST be `Some(_)` —
/// V1 closed-mode requires a Bearer token. Open mode (no token)
/// returns `PgError::NoTokenConfigured` BEFORE reading any client
/// bytes; the server.rs listener should not even spawn a thread
/// for the connection if `ServerConfig.token` is unset (mirrors
/// HTTP gateway's auth-on-startup posture).
///
/// **Deterministic-nonce knob:** `server_nonce_fn` is invoked once
/// to produce the per-session SCRAM server nonce. Production callers
/// pass a CSPRNG-backed closure; tests pass a constant-string
/// closure for KAT reproducibility.
pub fn accept<S: Read + Write, F: FnOnce() -> String>(
    stream: &mut S,
    token: Option<&[u8]>,
    server_nonce_fn: F,
) -> Result<AcceptedSession, PgError> {
    // Spec §3.4: V1 closed-mode requires a Bearer token. Reject
    // connections BEFORE reading any client bytes.
    let token = token.ok_or(PgError::NoTokenConfigured)?;

    // ─── Startup phase ────────────────────────────────────────────
    // PG §55.2.1: the first message has NO type tag (just length).
    // The client may send SSLRequest / GSSENCRequest pre-handshake
    // BEFORE the real StartupMessage; in that case we reply 'N' and
    // loop back to read the actual StartupMessage.
    let startup = loop {
        let (_tag, frame) = read_message(stream, false)?;
        match classify_initial_message(&frame)? {
            InitialMessage::SslRequest => {
                stream.write_all(&[SSL_REPLY_NO_TLS])?;
                stream.flush()?;
                continue;
            }
            InitialMessage::GssEncRequest => {
                stream.write_all(&[GSS_REPLY_NO_GSS])?;
                stream.flush()?;
                continue;
            }
            InitialMessage::CancelRequest { .. } => {
                // V1 takes no action on CancelRequest (V2 SP-PG T24
                // wires the cancel-key table). PG §55.2.1 — the
                // canonical response is to drop the connection
                // without further reply.
                return Err(PgError::StartupFailed(
                    StartupError::MalformedBody {
                        reason: "CancelRequest — V1 does not action; closing",
                    },
                ));
            }
            InitialMessage::Startup(sm) => break sm,
        }
    };

    // ─── Auth phase: SCRAM-SHA-256 ─────────────────────────────────
    // Spec §3.3: send AuthenticationSASL challenge → read
    // SASLInitialResponse → send SASLContinue → read SASLResponse →
    // send SASLFinal → send AuthenticationOk.
    stream.write_all(&encode_authentication_sasl_challenge())?;
    stream.flush()?;

    // SASLInitialResponse (p-tag frame; payload = mech\0 + len:u32 + client_first)
    let (tag, frame) = read_message(stream, true)?;
    if tag != FE_PASSWORD {
        return Err(PgError::UnexpectedMessageDuringAuth { tag });
    }
    // frame = [length:4][body]; payload = body
    let payload = &frame[4..];
    let (_mech, client_first) = auth::parse_sasl_initial_response(payload)?;

    let server_nonce = server_nonce_fn();
    let (server_first, state) = auth::start_scram(
        &client_first, token, &server_nonce, PG_DEFAULT_SCRAM_ITERATIONS,
    )?;
    stream.write_all(&encode_authentication_sasl_continue(&server_first))?;
    stream.flush()?;

    // SASLResponse (p-tag frame; payload = client_final bytes verbatim)
    let (tag, frame) = read_message(stream, true)?;
    if tag != FE_PASSWORD {
        return Err(PgError::UnexpectedMessageDuringAuth { tag });
    }
    let payload = &frame[4..];
    let client_final = std::str::from_utf8(payload)
        .map_err(|_| PgError::AuthFailed(AuthError::MalformedClientFinal))?;
    let server_final = auth::finish_scram(client_final, &state, token)?;
    stream.write_all(&encode_authentication_sasl_final(&server_final))?;
    stream.write_all(&encode_authentication_ok())?;

    // ─── Post-auth greeting (spec §8.4 / PG §55.2.6) ───────────────
    write_parameter_status(stream, "server_version", "14.0 (KesselDB SP-PG V1)")?;
    write_parameter_status(stream, "server_encoding", "UTF8")?;
    write_parameter_status(stream, "client_encoding", "UTF8")?;
    write_parameter_status(stream, "DateStyle", "ISO, MDY")?;
    write_parameter_status(stream, "TimeZone", "UTC")?;
    write_parameter_status(stream, "integer_datetimes", "on")?;
    write_parameter_status(stream, "standard_conforming_strings", "on")?;
    if let Some(app) = startup.get_param("application_name") {
        write_parameter_status(stream, "application_name", app)?;
    } else {
        write_parameter_status(stream, "application_name", "")?;
    }

    // BackendKeyData — per spec §3.4 open question #4, V1 derives
    // pid + secret deterministically from the server nonce + token
    // (no global cancel-key table yet). T2 ships the wire bytes; T24
    // (V2) wires the actual table.
    let pid_secret = pid_and_secret_from_nonce(&server_nonce, token);
    let (pid, secret) = pid_secret;
    write_backend_key_data(stream, pid, secret)?;
    write_ready_for_query(stream, READY_FOR_QUERY_IDLE)?;
    stream.flush()?;

    Ok(AcceptedSession {
        user: startup.user,
        pid,
        secret,
    })
}

/// Full per-connection session: runs the handshake via `accept`, then
/// enters the Simple-Query loop until the client sends `Terminate`
/// ('X') or the TCP connection drops.
///
/// This is the entry point a real PG-wire listener (T12 wires it up
/// behind the `pg-gateway` feature) will call once per accepted TCP
/// connection.
///
/// Loop body per spec §8 + PG §55.2.3:
///
/// 1. Read the next message tag byte from the stream.
/// 2. `'Q'` (Simple Query) → parse body via T3, dispatch via
///    `dispatch::dispatch_query`, write the response bytes (RowDescription
///    + DataRow* + CommandComplete + ReadyForQuery, OR EmptyQueryResponse
///    + ReadyForQuery, OR ErrorResponse + ReadyForQuery).
/// 3. `'X'` (Terminate) → close connection cleanly, return `Ok(())`.
/// 4. Any other tag → write ErrorResponse `08P01` (protocol_violation)
///    + close. (Per spec §11 weak-spot #5, V1 rejects extended-query
///    messages with a clean error — V2 SP-PG-EXTQ implements them.)
///
/// Returns `Ok(())` for a clean session close (Terminate or EOF) or
/// `Err(PgError)` for I/O or protocol failures.
pub fn run_session<
    S: Read + Write,
    F: FnOnce() -> String,
    E: crate::engine::EngineApply + ?Sized,
>(
    stream: &mut S,
    token: Option<&[u8]>,
    server_nonce_fn: F,
    engine: &E,
) -> Result<AcceptedSession, PgError> {
    // ─── Handshake ────────────────────────────────────────────────
    let session = accept(stream, token, server_nonce_fn)?;
    // ─── Per-connection Extended Query state (SP-PG-EXTQ §3) ──────
    // Created here, lives for the lifetime of the connection, drops
    // cleanly on return/Terminate/EOF. The state owns its statements
    // + portals — there is no global table to clean up.
    let mut extq_state = crate::extq::SessionState::new();
    // ─── Query loop ───────────────────────────────────────────────
    loop {
        let mut tag_buf = [0u8; 1];
        match stream.read_exact(&mut tag_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Clean EOF — client closed without Terminate.
                return Ok(session);
            }
            Err(e) if is_idle_timeout(e.kind()) => {
                // T16 (spec §9.3): the caller-installed
                // `set_read_timeout(pg_idle_timeout)` on the stream
                // fired BEFORE the client sent its next message tag.
                // Distinguish this from a peer-clean-close (EOF, above)
                // or a peer-RST (Io::ConnectionReset/BrokenPipe, below)
                // — only this case gets a typed FATAL 57014 frame
                // before the close, because only here is the SOCKET
                // STILL HEALTHY enough to receive a final write. EOF
                // means the peer already closed the read half; RST
                // means the kernel will swallow any further write.
                let frame = crate::error::encode_idle_timeout_error();
                // Best-effort: any write error here is silently
                // absorbed because we're about to drop the connection
                // anyway. The important thing is libpq either sees
                // the FATAL frame or sees the close — never a hang.
                let _ = stream.write_all(&frame);
                let _ = stream.flush();
                return Err(PgError::IdleTimeout);
            }
            Err(e) => return Err(PgError::Io(e.kind())),
        }
        let tag = tag_buf[0];
        // Read length-prefix.
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)?;
        let length = u32::from_be_bytes(len_buf);
        if length as usize > PG_MAX_MESSAGE_SIZE {
            return Err(PgError::MessageTooLarge { length });
        }
        if length < 4 {
            // length must include itself.
            let resp = crate::dispatch::error_response_then_rfq(
                crate::error::SEVERITY_ERROR,
                "08P01",
                "protocol violation: message length < 4",
            );
            let _ = stream.write_all(&resp);
            let _ = stream.flush();
            return Err(PgError::StartupFailed(StartupError::LengthTooSmall {
                length,
            }));
        }
        let body_len = (length as usize) - 4;
        let mut body = vec![0u8; body_len];
        stream.read_exact(&mut body)?;

        match tag {
            crate::proto::FE_QUERY => {
                // Parse Q body — strip trailing NUL, validate UTF-8.
                let sql_owned = match crate::query::parse_query_body(&body) {
                    Ok(s) => s.to_string(),
                    Err(_) => {
                        let resp = crate::dispatch::error_response_then_rfq(
                            crate::error::SEVERITY_ERROR,
                            "08P01",
                            "protocol violation: malformed Q body",
                        );
                        stream.write_all(&resp)?;
                        stream.flush()?;
                        continue;
                    }
                };
                let resp = crate::dispatch::dispatch_query(&sql_owned, engine);
                stream.write_all(&resp)?;
                stream.flush()?;
            }
            crate::proto::FE_TERMINATE => {
                // Clean close — no response per PG §55.2.3.
                return Ok(session);
            }
            other if crate::extq::recognize_extq_tag(other) => {
                // SP-PG-EXTQ T2 — Extended-Query message tag
                // recognized. Decode the body per the tag and route
                // into `extq::try_dispatch_extq`. T2 implements the
                // `P` Parse arm end-to-end (decode → dispatch →
                // ParseComplete bytes on the wire). The other six
                // tags (B/D/E/S/C/H) still render as `0A000`
                // NotYetImplemented — T3..T8 widen them per the
                // SP-PG-EXTQ §10 task decomposition.
                //
                // V1 contract preserved from T1: the connection
                // STAYS ALIVE across an extq tag rejection so
                // probe-then-fall-back clients (SQLAlchemy /
                // psycopg / JDBC) can degrade to Simple Query. The
                // V1 RFQ status byte is always `'I'` (no implicit-
                // tx semantics inside a Sync block; that's V2
                // SP-PG-TX).
                let decoded = match other {
                    crate::proto::FE_PARSE => crate::extq::proto::decode_parse(&body),
                    crate::proto::FE_BIND => crate::extq::proto::decode_bind(&body),
                    crate::proto::FE_DESCRIBE => crate::extq::proto::decode_describe(&body),
                    crate::proto::FE_EXECUTE => crate::extq::proto::decode_execute(&body),
                    crate::proto::FE_SYNC => crate::extq::proto::decode_sync(&body),
                    crate::proto::FE_CLOSE => crate::extq::proto::decode_close(&body),
                    crate::proto::FE_FLUSH => crate::extq::proto::decode_flush(&body),
                    _ => unreachable!("recognize_extq_tag accepted a tag not in the seven-tag set"),
                };
                let message = match decoded {
                    Ok(m) => m,
                    Err(_) => {
                        // Decoder rejected the body shape — `08P01
                        // protocol_violation`. Per SP-PG-EXTQ §6
                        // this should also set error_state so
                        // subsequent extq messages get skipped
                        // until Sync, but T7 owns the error-
                        // recovery state machine — T2 emits the
                        // single ErrorResponse + RFQ and lets the
                        // connection continue. Decoder rejection
                        // BEFORE the dispatcher runs means no
                        // state mutation happened either way.
                        let resp = crate::dispatch::error_response_then_rfq(
                            crate::error::SEVERITY_ERROR,
                            "08P01",
                            "protocol violation: malformed extended-query message body",
                        );
                        stream.write_all(&resp)?;
                        stream.flush()?;
                        continue;
                    }
                };
                let outcome = crate::extq::try_dispatch_extq(&mut extq_state, message);
                match outcome {
                    crate::extq::ExtqOutcome::Bytes(bytes) => {
                        // Successful dispatch — emit the encoded
                        // response frame verbatim. T2: only Parse
                        // reaches this arm; the bytes are
                        // ParseComplete (`1 [length=4]`). The
                        // PG spec §55.2.3 says ReadyForQuery is
                        // emitted only on Sync — but in V1 the
                        // wider ORM ecosystem also tolerates a
                        // RFQ after each extq message (eager-
                        // flush mode per §5). For T2 we emit
                        // ONLY the ParseComplete; the client's
                        // subsequent Sync (T7) emits the RFQ.
                        stream.write_all(&bytes)?;
                        stream.flush()?;
                    }
                    crate::extq::ExtqOutcome::Failed(err) => {
                        // Map the typed ExtqError → SQLSTATE per
                        // spec §6 + §7.1.
                        let (sqlstate, message) = match err {
                            crate::extq::ExtqError::NotYetImplemented { tag } => (
                                "0A000",
                                format!(
                                    "Extended Query message '{tag_char}' (0x{tag:02X}) not yet implemented (SP-PG-EXTQ in progress)",
                                    tag_char = tag as char,
                                ),
                            ),
                            crate::extq::ExtqError::Decode { reason } => (
                                "08P01",
                                format!("protocol violation: {reason}"),
                            ),
                            crate::extq::ExtqError::TooManyPreparedStatements => (
                                "08P01",
                                format!(
                                    "too many prepared statements (max {} per connection)",
                                    crate::extq::MAX_PREPARED_STATEMENTS_PER_CONN
                                ),
                            ),
                            crate::extq::ExtqError::TooManyPortals => (
                                "08P01",
                                format!(
                                    "too many portals (max {} per connection)",
                                    crate::extq::MAX_PORTALS_PER_CONN
                                ),
                            ),
                            crate::extq::ExtqError::BinaryFormatNotSupported { position } => (
                                "0A000",
                                format!(
                                    "binary-format parameters not supported in V1 (position {position}); client must request text-format (format code 0)"
                                ),
                            ),
                            crate::extq::ExtqError::UnknownStatement { name } => (
                                "26000",
                                format!("prepared statement \"{name}\" does not exist"),
                            ),
                            crate::extq::ExtqError::UnknownPortal { name } => (
                                "34000",
                                format!("portal \"{name}\" does not exist"),
                            ),
                            crate::extq::ExtqError::PreparedStatementAlreadyExists { name } => (
                                "42P05",
                                format!("prepared statement \"{name}\" already exists"),
                            ),
                        };
                        // Same "stay alive" contract as the T1
                        // branch — emit ErrorResponse + RFQ and
                        // continue. T7 will refine this for the
                        // skip-until-Sync semantics inside a
                        // pipelined block.
                        let resp = crate::dispatch::error_response_then_rfq(
                            crate::error::SEVERITY_ERROR,
                            sqlstate,
                            &message,
                        );
                        stream.write_all(&resp)?;
                        stream.flush()?;
                    }
                    crate::extq::ExtqOutcome::SyncCompleted => {
                        // T7 wires the Sync handler. Until then,
                        // Sync hits the NotYetImplemented arm
                        // above, so this branch is unreachable in
                        // T2 — but exhaustive `match` requires
                        // it. Emit RFQ('I') defensively so a
                        // future T7 refactor that switches `Sync`
                        // to return `SyncCompleted` doesn't
                        // silently break this call site.
                        let mut rfq = Vec::with_capacity(6);
                        rfq.extend_from_slice(&[crate::proto::BE_READY_FOR_QUERY, 0, 0, 0, 5, b'I']);
                        stream.write_all(&rfq)?;
                        stream.flush()?;
                    }
                }
                continue;
            }
            other => {
                // Unknown / unsupported message type. V1 rejects with
                // a clean error + closes the connection.
                let resp = crate::dispatch::error_response_then_rfq(
                    crate::error::SEVERITY_ERROR,
                    "08P01",
                    &format!("unsupported message tag: 0x{other:02X}"),
                );
                stream.write_all(&resp)?;
                stream.flush()?;
                return Err(PgError::UnexpectedMessageDuringAuth { tag: other });
            }
        }
    }
}

/// Writes a ParameterStatus message: `S [length:4 BE] [key\0] [value\0]`.
/// PG §55.2.6 — emitted after AuthenticationOk to announce server
/// session parameters the client should know about (`server_version`,
/// `server_encoding`, etc.).
fn write_parameter_status<W: Write>(w: &mut W, key: &str, value: &str) -> Result<(), PgError> {
    let payload_len = key.len() + 1 + value.len() + 1;
    let length = (4 + payload_len) as u32;
    w.write_all(&[BE_PARAMETER_STATUS])?;
    w.write_all(&length.to_be_bytes())?;
    w.write_all(key.as_bytes())?;
    w.write_all(&[0])?;
    w.write_all(value.as_bytes())?;
    w.write_all(&[0])?;
    Ok(())
}

/// Writes a BackendKeyData message: `K [length:4 BE = 12] [pid:u32 BE]
/// [secret:u32 BE]`. PG §55.2.6 / §55.2.10. V1 emits it but does NOT
/// action a subsequent CancelRequest (V2 SP-PG T24).
fn write_backend_key_data<W: Write>(w: &mut W, pid: u32, secret: u32) -> Result<(), PgError> {
    w.write_all(&[BE_BACKEND_KEY_DATA])?;
    w.write_all(&12u32.to_be_bytes())?;
    w.write_all(&pid.to_be_bytes())?;
    w.write_all(&secret.to_be_bytes())?;
    Ok(())
}

/// Writes a ReadyForQuery message: `Z [length:4 BE = 5] [status:1]`.
/// V1 always emits status='I' (idle — no transaction in progress);
/// V2 would emit 'T'/'E' once BEGIN/COMMIT/ROLLBACK awareness lands.
fn write_ready_for_query<W: Write>(w: &mut W, status: u8) -> Result<(), PgError> {
    w.write_all(&[BE_READY_FOR_QUERY])?;
    w.write_all(&5u32.to_be_bytes())?;
    w.write_all(&[status])?;
    Ok(())
}

/// Derives BackendKeyData (pid, secret) deterministically from the
/// per-session SCRAM server nonce + the operator's Bearer token.
/// Spec §3.4 open question #4 — V1 doesn't have a global cancel-key
/// table, so we surface SOMETHING in BackendKeyData (clients log
/// it; some clients refuse a connection that doesn't send one) but
/// take no action on a subsequent CancelRequest. V2 SP-PG T24 wires
/// the real table and replaces this function.
///
/// The derivation: `digest = SHA-256(server_nonce || token)`;
/// `pid = u32(digest[..4])`; `secret = u32(digest[4..8])`. PIDs
/// less than 16 are bumped to avoid colliding with kernel-reserved
/// PIDs that some old psql versions special-case.
fn pid_and_secret_from_nonce(nonce: &str, token: &[u8]) -> (u32, u32) {
    let mut input: Vec<u8> = Vec::with_capacity(nonce.len() + token.len());
    input.extend_from_slice(nonce.as_bytes());
    input.extend_from_slice(token);
    let digest = kessel_crypto::sha256(&input);
    let mut pid = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
    if pid < 16 {
        pid = pid.wrapping_add(16);
    }
    let secret = u32::from_be_bytes([digest[4], digest[5], digest[6], digest[7]]);
    (pid, secret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::encode_authentication_sasl_challenge;
    use kessel_crypto::{base64_encode, hmac_sha256, pbkdf2_hmac_sha256, sha256};
    use std::io::Cursor;

    /// In-memory duplex stream: reads pull from `inbound`, writes
    /// push to `outbound`. Test SCRAM clients build a stream of
    /// pre-canned inbound bytes + drive `accept` then inspect the
    /// outbound buffer for the expected response bytes.
    struct Pipe {
        inbound: Cursor<Vec<u8>>,
        outbound: Vec<u8>,
    }

    impl Pipe {
        fn new(inbound: Vec<u8>) -> Self {
            Self { inbound: Cursor::new(inbound), outbound: Vec::new() }
        }
    }

    impl Read for Pipe {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.inbound.read(buf)
        }
    }

    impl Write for Pipe {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.outbound.write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }

    /// Build a StartupMessage frame matching what libpq sends for
    /// `psql -U test`.
    fn build_startup_frame(user: &str) -> Vec<u8> {
        let body = format!("user\0{user}\0\0");
        let length = (4 + 4 + body.len()) as u32;
        let mut frame = Vec::new();
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&196608u32.to_be_bytes());
        frame.extend_from_slice(body.as_bytes());
        frame
    }

    /// Build a SASLInitialResponse `p`-frame.
    /// Wire: `p [length:4][SCRAM-SHA-256\0][client_first_len:u32][client_first]`
    fn build_sasl_initial_frame(client_first: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"SCRAM-SHA-256\0");
        payload.extend_from_slice(&(client_first.len() as u32).to_be_bytes());
        payload.extend_from_slice(client_first.as_bytes());
        let length = (4 + payload.len()) as u32;
        let mut frame = Vec::new();
        frame.push(b'p');
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    /// Build a SASLResponse `p`-frame containing the client-final.
    fn build_sasl_response_frame(client_final: &str) -> Vec<u8> {
        let payload = client_final.as_bytes();
        let length = (4 + payload.len()) as u32;
        let mut frame = Vec::new();
        frame.push(b'p');
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    /// Client-side SCRAM proof construction for tests. Mirrors what
    /// libpq does internally during a `PGPASSWORD=...` connection.
    fn compute_client_proof(
        token: &[u8],
        salt: &[u8],
        iterations: u32,
        auth_message: &str,
    ) -> (String, [u8; 32]) {
        let salted = pbkdf2_hmac_sha256(token, salt, iterations);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let client_sig = hmac_sha256(&stored_key, auth_message.as_bytes());
        let mut proof = [0u8; 32];
        for i in 0..32 {
            proof[i] = client_key[i] ^ client_sig[i];
        }
        (base64_encode(&proof), client_key)
    }

    // ─── Headline KAT: full successful SCRAM round-trip via accept ──

    /// T2 flips the T1 stub. The original
    /// `t1_accept_returns_not_yet_implemented_stub` is replaced by
    /// this test: a successful SCRAM-SHA-256 round-trip over an
    /// in-memory pipe, asserting that `accept` returns
    /// `Ok(AcceptedSession)` with the right user and a non-zero
    /// (pid, secret) pair, and that the outbound bytes contain the
    /// expected post-auth greeting sequence.
    #[test]
    fn t2_accept_runs_full_scram_handshake_to_ready_for_query() {
        let token = b"kessel-bearer-token";
        let client_nonce = "fixedClientNonce";
        let server_nonce = "fixedServerNonce";
        let username = "test";

        // Pre-compute the client side of SCRAM so we can build a
        // canned inbound byte stream.
        let client_first_bare = format!("n={username},r={client_nonce}");
        let client_first = format!("n,,{client_first_bare}");

        // The server will derive:
        //   salt = SHA-256(server_nonce || token)[..16]
        //   server_first = "r={client_nonce}{server_nonce},s={salt_b64},i=4096"
        let mut salt_input = Vec::new();
        salt_input.extend_from_slice(server_nonce.as_bytes());
        salt_input.extend_from_slice(token);
        let salt: Vec<u8> = sha256(&salt_input)[..16].to_vec();
        let salt_b64 = base64_encode(&salt);
        let combined = format!("{client_nonce}{server_nonce}");
        let server_first = format!("r={combined},s={salt_b64},i=4096");
        let cf_without_proof = format!("c=biws,r={combined}");
        let auth_msg =
            format!("{client_first_bare},{server_first},{cf_without_proof}");
        let (proof_b64, _client_key) = compute_client_proof(token, &salt, 4096, &auth_msg);
        let client_final = format!("{cf_without_proof},p={proof_b64}");

        // Build the inbound byte stream the server will read in order:
        //   1. StartupMessage
        //   2. SASLInitialResponse (p-tag)
        //   3. SASLResponse (p-tag)
        let mut inbound = Vec::new();
        inbound.extend_from_slice(&build_startup_frame(username));
        inbound.extend_from_slice(&build_sasl_initial_frame(&client_first));
        inbound.extend_from_slice(&build_sasl_response_frame(&client_final));

        let mut pipe = Pipe::new(inbound);
        let session = accept(&mut pipe, Some(token), || server_nonce.to_string())
            .expect("SCRAM handshake completes against the in-memory pipe");
        assert_eq!(session.user, username);
        assert_ne!(session.pid, 0);
        // pid >= 16 per the kernel-PID-collision avoidance rule
        assert!(session.pid >= 16);

        // Outbound bytes (in order):
        //  - AuthenticationSASL challenge (24 bytes)
        //  - AuthenticationSASLContinue (R-envelope wrapping server-first)
        //  - AuthenticationSASLFinal (R-envelope wrapping "v=...")
        //  - AuthenticationOk (9 bytes)
        //  - 8 ParameterStatus messages
        //  - BackendKeyData (13 bytes: 'K' + length=12 + pid + secret)
        //  - ReadyForQuery (6 bytes: 'Z' + length=5 + 'I')
        let out = &pipe.outbound;

        // First 24 bytes are the AuthenticationSASL challenge
        let expected_challenge = encode_authentication_sasl_challenge();
        assert_eq!(&out[..24], &expected_challenge[..]);

        // Find the AuthenticationOk byte sequence (R, 0,0,0,8, 0,0,0,0).
        let auth_ok = &[b'R', 0, 0, 0, 8, 0, 0, 0, 0][..];
        assert!(
            out.windows(9).any(|w| w == auth_ok),
            "AuthenticationOk byte sequence MUST appear in outbound bytes"
        );

        // Find ReadyForQuery: 'Z', 0,0,0,5, 'I'
        let rfq = &[b'Z', 0, 0, 0, 5, b'I'][..];
        assert!(
            out.windows(6).any(|w| w == rfq),
            "ReadyForQuery ('Z' [len=5] 'I') MUST appear in outbound bytes"
        );

        // ParameterStatus server_version + UTF8 encoding present
        assert!(
            out.windows(b"server_version".len()).any(|w| w == b"server_version"),
            "ParameterStatus(server_version=...) MUST appear in outbound bytes"
        );
        assert!(
            out.windows(b"UTF8".len()).any(|w| w == b"UTF8"),
            "server_encoding=UTF8 MUST appear in outbound bytes"
        );

        // BackendKeyData: 'K' + length=12 + 8 bytes
        let pid_be = session.pid.to_be_bytes();
        let secret_be = session.secret.to_be_bytes();
        let mut bkd = vec![b'K', 0, 0, 0, 12];
        bkd.extend_from_slice(&pid_be);
        bkd.extend_from_slice(&secret_be);
        assert!(
            out.windows(13).any(|w| w == bkd.as_slice()),
            "BackendKeyData with the announced (pid, secret) MUST appear in outbound bytes"
        );

        // Order invariant: AuthenticationOk comes BEFORE ReadyForQuery.
        let ok_pos = out
            .windows(9)
            .position(|w| w == auth_ok)
            .expect("AuthenticationOk present");
        let rfq_pos = out
            .windows(6)
            .position(|w| w == rfq)
            .expect("ReadyForQuery present");
        assert!(
            ok_pos < rfq_pos,
            "AuthenticationOk MUST precede ReadyForQuery in outbound bytes"
        );
    }

    /// `accept` rejects connections when `token` is `None` BEFORE
    /// reading any client bytes. Spec §3.4: V1 closed-mode requires
    /// a Bearer token; open mode returns `NoTokenConfigured` (the
    /// listener should not even spawn a thread for the connection).
    #[test]
    fn t2_accept_rejects_when_no_token_configured() {
        let mut pipe = Pipe::new(Vec::new());
        match accept(&mut pipe, None, || "irrelevant".to_string()) {
            Err(PgError::NoTokenConfigured) => {}
            other => panic!("expected NoTokenConfigured, got {other:?}"),
        }
        // No bytes touched on the stream — the rejection is pre-read.
        assert_eq!(pipe.outbound.len(), 0);
    }

    /// SSLRequest pre-handshake → server replies 'N' and loops back
    /// to read the real StartupMessage; the SCRAM exchange proceeds
    /// normally. Locks the §3.2 SSL-redirect-then-handshake invariant.
    #[test]
    fn t2_accept_handles_ssl_request_then_completes_handshake() {
        let token = b"kessel-bearer-token";
        let client_nonce = "fixedClientNonce";
        let server_nonce = "fixedServerNonce";
        let username = "test";

        // Pre-build the SCRAM bytes
        let client_first_bare = format!("n={username},r={client_nonce}");
        let client_first = format!("n,,{client_first_bare}");
        let mut salt_input = Vec::new();
        salt_input.extend_from_slice(server_nonce.as_bytes());
        salt_input.extend_from_slice(token);
        let salt: Vec<u8> = sha256(&salt_input)[..16].to_vec();
        let salt_b64 = base64_encode(&salt);
        let combined = format!("{client_nonce}{server_nonce}");
        let server_first = format!("r={combined},s={salt_b64},i=4096");
        let cf_without_proof = format!("c=biws,r={combined}");
        let auth_msg =
            format!("{client_first_bare},{server_first},{cf_without_proof}");
        let (proof_b64, _) = compute_client_proof(token, &salt, 4096, &auth_msg);
        let client_final = format!("{cf_without_proof},p={proof_b64}");

        // Inbound stream: SSLRequest first, THEN StartupMessage + SCRAM.
        let mut inbound = Vec::new();
        // SSLRequest: length=8, code=80877103
        inbound.extend_from_slice(&8u32.to_be_bytes());
        inbound.extend_from_slice(&80877103u32.to_be_bytes());
        inbound.extend_from_slice(&build_startup_frame(username));
        inbound.extend_from_slice(&build_sasl_initial_frame(&client_first));
        inbound.extend_from_slice(&build_sasl_response_frame(&client_final));

        let mut pipe = Pipe::new(inbound);
        let session = accept(&mut pipe, Some(token), || server_nonce.to_string())
            .expect("SSLRequest then SCRAM handshake completes");
        assert_eq!(session.user, username);

        // The first outbound byte MUST be 'N' (no TLS).
        assert_eq!(pipe.outbound[0], b'N');
        // Followed by the AuthenticationSASL challenge starting at byte 1.
        let expected = encode_authentication_sasl_challenge();
        assert_eq!(&pipe.outbound[1..1 + expected.len()], &expected[..]);
    }

    /// Bad client proof (wrong token) → `PgError::AuthFailed
    /// (ProofVerificationFailed)`. Server should NOT have sent
    /// AuthenticationOk (no false positive); should NOT have sent
    /// ReadyForQuery (no oracle).
    #[test]
    fn t2_accept_bad_proof_returns_auth_failed_no_ready_for_query() {
        let real_token = b"real-token";
        let wrong_token = b"WRONG-token";
        let client_nonce = "clientN";
        let server_nonce = "serverN";
        let username = "test";

        let client_first_bare = format!("n={username},r={client_nonce}");
        let client_first = format!("n,,{client_first_bare}");
        // Compute proof against the WRONG token — server will reject.
        let mut salt_input = Vec::new();
        salt_input.extend_from_slice(server_nonce.as_bytes());
        salt_input.extend_from_slice(real_token);
        let salt: Vec<u8> = sha256(&salt_input)[..16].to_vec();
        let salt_b64 = base64_encode(&salt);
        let combined = format!("{client_nonce}{server_nonce}");
        let server_first = format!("r={combined},s={salt_b64},i=4096");
        let cf_without_proof = format!("c=biws,r={combined}");
        let auth_msg =
            format!("{client_first_bare},{server_first},{cf_without_proof}");
        let (proof_b64, _) =
            compute_client_proof(wrong_token, &salt, 4096, &auth_msg);
        let client_final = format!("{cf_without_proof},p={proof_b64}");

        let mut inbound = Vec::new();
        inbound.extend_from_slice(&build_startup_frame(username));
        inbound.extend_from_slice(&build_sasl_initial_frame(&client_first));
        inbound.extend_from_slice(&build_sasl_response_frame(&client_final));

        let mut pipe = Pipe::new(inbound);
        match accept(&mut pipe, Some(real_token), || server_nonce.to_string()) {
            Err(PgError::AuthFailed(AuthError::ProofVerificationFailed)) => {}
            other => panic!("expected AuthFailed(ProofVerificationFailed), got {other:?}"),
        }
        // No AuthenticationOk in the outbound (server rejected before emitting it).
        let auth_ok = &[b'R', 0, 0, 0, 8, 0, 0, 0, 0][..];
        assert!(
            !pipe.outbound.windows(9).any(|w| w == auth_ok),
            "AuthenticationOk MUST NOT appear after a failed proof"
        );
        // No ReadyForQuery either.
        let rfq = &[b'Z', 0, 0, 0, 5, b'I'][..];
        assert!(
            !pipe.outbound.windows(6).any(|w| w == rfq),
            "ReadyForQuery MUST NOT appear after a failed proof"
        );
    }

    /// EOF before StartupMessage → `PgError::Io(UnexpectedEof)`.
    /// Locked behavior — the connection died before the client
    /// could send the first byte; server-loop drops the thread.
    #[test]
    fn t2_accept_eof_before_startup_is_io_error() {
        let mut pipe = Pipe::new(Vec::new());
        match accept(&mut pipe, Some(b"token"), || "nonce".to_string()) {
            Err(PgError::Io(std::io::ErrorKind::UnexpectedEof)) => {}
            other => panic!("expected Io(UnexpectedEof), got {other:?}"),
        }
    }

    /// `pid_and_secret_from_nonce` is deterministic — same inputs
    /// produce same (pid, secret) — and bumps pids < 16. Locks the
    /// spec §3.4 derivation rule against a refactor.
    #[test]
    fn t2_backend_key_data_derivation_is_deterministic() {
        let token = b"some-token";
        let nonce = "some-nonce";
        let (pid_a, secret_a) = pid_and_secret_from_nonce(nonce, token);
        let (pid_b, secret_b) = pid_and_secret_from_nonce(nonce, token);
        assert_eq!(pid_a, pid_b);
        assert_eq!(secret_a, secret_b);
        assert!(pid_a >= 16, "kernel-reserved PIDs avoided");
    }

    /// `pid_and_secret_from_nonce` produces DIFFERENT pairs for
    /// different nonces (entropy from the per-session nonce).
    /// Locked because a constant pair across sessions would defeat
    /// the cancel-key replay-prevention story V2 will rely on.
    #[test]
    fn t2_backend_key_data_changes_across_nonces() {
        let token = b"some-token";
        let (pid_a, secret_a) = pid_and_secret_from_nonce("nonce-A", token);
        let (pid_b, secret_b) = pid_and_secret_from_nonce("nonce-B", token);
        assert!(
            pid_a != pid_b || secret_a != secret_b,
            "different nonces MUST produce different BackendKeyData"
        );
    }

    // ───────────────────────────────────────────────────────────────────
    // T8 KATs — full session loop: handshake + Q dispatch + Terminate.
    // ───────────────────────────────────────────────────────────────────

    /// A minimal test engine used by the T8 session-loop KATs.
    /// Always returns an empty SELECT (0 rows) so we can focus on
    /// the framing without record-encoding noise.
    struct EmptySelectEngine;
    impl crate::engine::EngineApply for EmptySelectEngine {
        fn apply_sql(&self, _sql: &str) -> kessel_proto::OpResult {
            kessel_proto::OpResult::Got(Vec::new())
        }
        fn describe_table(
            &self,
            name: &str,
        ) -> Option<Vec<crate::engine::PgColumn>> {
            if name == "t" {
                Some(vec![crate::engine::PgColumn {
                    name: "id".into(),
                    kind: kessel_catalog::FieldKind::I64,
                    nullable: false,
                }])
            } else {
                None
            }
        }
    }

    /// Build a 'Q' simple-query frame: `Q [length:4 BE] [sql\0]`.
    fn build_q_frame(sql: &str) -> Vec<u8> {
        let mut payload = sql.as_bytes().to_vec();
        payload.push(0);
        let length = (4 + payload.len()) as u32;
        let mut frame = Vec::new();
        frame.push(b'Q');
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    /// Build a Terminate 'X' frame: `X [length:4 BE = 4]`.
    fn build_x_frame() -> Vec<u8> {
        vec![b'X', 0, 0, 0, 4]
    }

    /// Build the full inbound byte stream a session expects: a
    /// successful SCRAM handshake followed by additional frames.
    fn build_authed_inbound(
        token: &[u8],
        client_nonce: &str,
        server_nonce: &str,
        username: &str,
        extra: &[u8],
    ) -> Vec<u8> {
        let client_first_bare = format!("n={username},r={client_nonce}");
        let client_first = format!("n,,{client_first_bare}");
        let mut salt_input = Vec::new();
        salt_input.extend_from_slice(server_nonce.as_bytes());
        salt_input.extend_from_slice(token);
        let salt: Vec<u8> = kessel_crypto::sha256(&salt_input)[..16].to_vec();
        let salt_b64 = kessel_crypto::base64_encode(&salt);
        let combined = format!("{client_nonce}{server_nonce}");
        let server_first = format!("r={combined},s={salt_b64},i=4096");
        let cf_without_proof = format!("c=biws,r={combined}");
        let auth_msg =
            format!("{client_first_bare},{server_first},{cf_without_proof}");
        // Mirror the test-internal compute_client_proof:
        let salted = kessel_crypto::pbkdf2_hmac_sha256(token, &salt, 4096);
        let client_key = kessel_crypto::hmac_sha256(&salted, b"Client Key");
        let stored_key = kessel_crypto::sha256(&client_key);
        let client_sig = kessel_crypto::hmac_sha256(&stored_key, auth_msg.as_bytes());
        let mut proof = [0u8; 32];
        for i in 0..32 {
            proof[i] = client_key[i] ^ client_sig[i];
        }
        let proof_b64 = kessel_crypto::base64_encode(&proof);
        let client_final = format!("{cf_without_proof},p={proof_b64}");

        let mut inbound = Vec::new();
        inbound.extend_from_slice(&build_startup_frame(username));
        inbound.extend_from_slice(&build_sasl_initial_frame(&client_first));
        inbound.extend_from_slice(&build_sasl_response_frame(&client_final));
        inbound.extend_from_slice(extra);
        inbound
    }

    /// Headline T8 KAT: a full session — handshake + `SELECT * FROM t`
    /// + Terminate — returns the expected backend byte sequence with
    /// the SELECT response embedded.
    #[test]
    fn t8_run_session_full_select_round_trip() {
        let token = b"kessel-bearer-token";
        let mut extra = Vec::new();
        extra.extend_from_slice(&build_q_frame("SELECT * FROM t"));
        extra.extend_from_slice(&build_x_frame());
        let inbound = build_authed_inbound(
            token,
            "clientN",
            "serverN",
            "test",
            &extra,
        );
        let mut pipe = Pipe::new(inbound);
        let engine = EmptySelectEngine;
        let session = run_session(
            &mut pipe,
            Some(token),
            || "serverN".to_string(),
            &engine,
        )
        .expect("session completes through Terminate");
        assert_eq!(session.user, "test");
        // The outbound bytes must contain:
        //   - the handshake greeting (ParameterStatus, BackendKeyData, RFQ)
        //   - then a RowDescription ('T') for the SELECT
        //   - then CommandComplete "SELECT 0"
        //   - then ReadyForQuery ('Z' [len=5] 'I')
        // The greeting ends with one RFQ — there should be TWO total
        // RFQ envelopes in the outbound bytes (greeting + post-query).
        let out = &pipe.outbound;
        let rfq = &[b'Z', 0, 0, 0, 5, b'I'][..];
        let mut rfq_count = 0;
        for w in out.windows(6) {
            if w == rfq {
                rfq_count += 1;
            }
        }
        assert!(rfq_count >= 2, "at least 2 ReadyForQuery envelopes expected");
        // RowDescription type byte appears AFTER the greeting RFQ.
        assert!(out.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }

    /// `Terminate` ('X') message → session closes cleanly without
    /// emitting any extra response.
    #[test]
    fn t8_run_session_terminate_closes_cleanly() {
        let token = b"kessel-bearer-token";
        let extra = build_x_frame();
        let inbound = build_authed_inbound(
            token,
            "clientN",
            "serverN",
            "test",
            &extra,
        );
        let mut pipe = Pipe::new(inbound);
        let engine = EmptySelectEngine;
        let session = run_session(
            &mut pipe,
            Some(token),
            || "serverN".to_string(),
            &engine,
        )
        .expect("session terminates cleanly");
        assert_eq!(session.user, "test");
    }

    /// SP-PG-EXTQ T2 — extended-query 'P' Parse with a valid body
    /// is now decoded + dispatched and produces the byte-locked
    /// 5-byte ParseComplete envelope (`1 [length=4]`) on the wire.
    /// No `0A000` (the T1 placeholder behavior) — Parse is now a
    /// real handler. No `08P01` (the pre-SP-PG-EXTQ V1 close-on-
    /// extq behavior). The session stays alive through the Parse
    /// and the subsequent Terminate closes cleanly.
    ///
    /// Headline KAT — the SP-PG-EXTQ §13 acceptance criteria #2
    /// (psql `\bind` extended-query path emits a parseable
    /// response) depends on this byte sequence.
    #[test]
    fn t2_extq_run_session_parse_tag_emits_parse_complete() {
        let token = b"kessel-bearer-token";
        // Extended-query 'P' Parse body:
        //   name="" + "\0" + sql="SELECT 1" + "\0" + param_count=0 (i16 BE)
        // → 12 bytes payload total.
        let mut extra = {
            let payload: &[u8] = b"\0SELECT 1\0\0\0";
            let length = (4 + payload.len()) as u32;
            let mut f = Vec::new();
            f.push(b'P');
            f.extend_from_slice(&length.to_be_bytes());
            f.extend_from_slice(payload);
            f
        };
        extra.extend_from_slice(&build_x_frame());
        let inbound = build_authed_inbound(
            token,
            "clientN",
            "serverN",
            "test",
            &extra,
        );
        let mut pipe = Pipe::new(inbound);
        let engine = EmptySelectEngine;
        let session = run_session(
            &mut pipe,
            Some(token),
            || "serverN".to_string(),
            &engine,
        )
        .expect("session stays alive across Parse + Terminate");
        assert_eq!(session.user, "test");

        let out = &pipe.outbound;
        // ParseComplete must appear in the outbound stream — locked
        // byte-for-byte against spec §9.
        let parse_complete: &[u8] = &[b'1', 0, 0, 0, 4];
        assert!(
            out.windows(parse_complete.len()).any(|w| w == parse_complete),
            "outbound must carry the 5-byte ParseComplete envelope"
        );
        // The pre-T2 0A000 NYI ErrorResponse must NOT appear.
        assert!(
            !out.windows(5).any(|w| w == b"0A000"),
            "T2 Parse should NOT emit 0A000 — it's a real handler now"
        );
        // The pre-SP-PG-EXTQ 08P01 must NOT appear either.
        assert!(
            !out.windows(5).any(|w| w == b"08P01"),
            "Parse on the extq path must NEVER emit 08P01"
        );
    }

    /// SP-PG-EXTQ T2 — extended-query 'B' Bind message (one of the
    /// six still-NYI tags) STILL renders `0A000 feature_not_supported`
    /// + RFQ and keeps the session alive. Locks the "T2 hasn't
    /// half-shipped T3" invariant — when T3 lands, this test flips
    /// to assert BindComplete (`2 [length=4]`) instead. Mirror tests
    /// for D/E/S/C/H land alongside their own slices.
    #[test]
    fn t2_extq_run_session_bind_tag_still_emits_0a000_and_stays_alive() {
        let token = b"kessel-bearer-token";
        // Minimal valid 'B' Bind body:
        //   portal="" + NUL + stmt="" + NUL + pf_count=0 (i16 BE) +
        //   pv_count=0 (i16 BE) + rf_count=0 (i16 BE)
        // → 8 bytes payload.
        let mut extra = {
            let mut payload = Vec::new();
            payload.push(0); // portal
            payload.push(0); // stmt
            payload.extend_from_slice(&0i16.to_be_bytes()); // pf_count
            payload.extend_from_slice(&0i16.to_be_bytes()); // pv_count
            payload.extend_from_slice(&0i16.to_be_bytes()); // rf_count
            let length = (4 + payload.len()) as u32;
            let mut f = Vec::new();
            f.push(b'B');
            f.extend_from_slice(&length.to_be_bytes());
            f.extend_from_slice(&payload);
            f
        };
        extra.extend_from_slice(&build_x_frame());
        let inbound = build_authed_inbound(
            token,
            "clientN",
            "serverN",
            "test",
            &extra,
        );
        let mut pipe = Pipe::new(inbound);
        let engine = EmptySelectEngine;
        let session = run_session(
            &mut pipe,
            Some(token),
            || "serverN".to_string(),
            &engine,
        )
        .expect("session stays alive across Bind NYI + Terminate");
        assert_eq!(session.user, "test");
        let out = &pipe.outbound;
        assert!(
            out.windows(5).any(|w| w == b"0A000"),
            "T2 Bind should still emit 0A000 NYI (until T3)"
        );
        // 0A000, not 08P01 — Bind is on the extq path so it must
        // stay alive.
        assert!(
            !out.windows(5).any(|w| w == b"08P01"),
            "Bind on the extq path must NOT emit 08P01"
        );
    }

    /// SP-PG-EXTQ T2 — a Parse body that the decoder REJECTS (e.g.
    /// missing-NUL in the name cstring) emits `08P01 protocol_
    /// violation` + RFQ. The session STAYS ALIVE — a malformed
    /// extq frame is still on the extq path; the connection isn't
    /// closed. Locks the decoder-reject error path against future
    /// drift to e.g. `0A000` or a connection close.
    #[test]
    fn t2_extq_run_session_parse_malformed_body_emits_08p01_and_stays_alive() {
        let token = b"kessel-bearer-token";
        // Malformed Parse body: 4 bytes "user" with NO NUL terminator
        // — the cstring decoder rejects with MissingNul.
        let mut extra = {
            let payload: &[u8] = b"user";
            let length = (4 + payload.len()) as u32;
            let mut f = Vec::new();
            f.push(b'P');
            f.extend_from_slice(&length.to_be_bytes());
            f.extend_from_slice(payload);
            f
        };
        extra.extend_from_slice(&build_x_frame());
        let inbound = build_authed_inbound(
            token,
            "clientN",
            "serverN",
            "test",
            &extra,
        );
        let mut pipe = Pipe::new(inbound);
        let engine = EmptySelectEngine;
        let session = run_session(
            &mut pipe,
            Some(token),
            || "serverN".to_string(),
            &engine,
        )
        .expect("session stays alive across malformed Parse + Terminate");
        assert_eq!(session.user, "test");
        assert!(
            pipe.outbound.windows(5).any(|w| w == b"08P01"),
            "outbound must contain SQLSTATE 08P01 for the decoder rejection"
        );
        // 5-byte ParseComplete must NOT appear (the dispatcher
        // never ran on a malformed body).
        let parse_complete: &[u8] = &[b'1', 0, 0, 0, 4];
        assert!(
            !pipe.outbound.windows(parse_complete.len()).any(|w| w == parse_complete),
            "ParseComplete must NOT appear when decoder rejects"
        );
    }

    /// SP-PG-EXTQ T1 — an UNRECOGNIZED message tag (neither Q, X, nor
    /// one of the seven extq tags) still closes the connection with
    /// `08P01 protocol_violation`. Locks the existing "true protocol
    /// violation = close" invariant against the new "extq tag = stay
    /// alive" branch.
    #[test]
    fn t1_run_session_genuinely_unknown_tag_still_closes_with_08p01() {
        let token = b"kessel-bearer-token";
        // 'Z' is a backend-only tag — a client sending it is a real
        // protocol violation (not just an unsupported feature).
        let extra = {
            let payload: &[u8] = &[];
            let length = (4 + payload.len()) as u32;
            let mut f = Vec::new();
            f.push(b'Z');
            f.extend_from_slice(&length.to_be_bytes());
            f.extend_from_slice(payload);
            f
        };
        let inbound = build_authed_inbound(
            token,
            "clientN",
            "serverN",
            "test",
            &extra,
        );
        let mut pipe = Pipe::new(inbound);
        let engine = EmptySelectEngine;
        let r = run_session(
            &mut pipe,
            Some(token),
            || "serverN".to_string(),
            &engine,
        );
        assert!(matches!(r, Err(PgError::UnexpectedMessageDuringAuth { tag: b'Z' })));
        assert!(pipe.outbound.windows(5).any(|w| w == b"08P01"));
    }

    /// Empty Q (whitespace-only SQL) emits EmptyQueryResponse + RFQ,
    /// the session stays alive, then Terminate closes it cleanly.
    #[test]
    fn t8_run_session_empty_q_then_terminate() {
        let token = b"kessel-bearer-token";
        let mut extra = Vec::new();
        extra.extend_from_slice(&build_q_frame("   "));
        extra.extend_from_slice(&build_x_frame());
        let inbound = build_authed_inbound(
            token,
            "clientN",
            "serverN",
            "test",
            &extra,
        );
        let mut pipe = Pipe::new(inbound);
        let engine = EmptySelectEngine;
        run_session(
            &mut pipe,
            Some(token),
            || "serverN".to_string(),
            &engine,
        )
        .expect("session completes");
        // EmptyQueryResponse 'I' (5 bytes total) present.
        let eqr = &[b'I', 0, 0, 0, 4][..];
        assert!(pipe.outbound.windows(5).any(|w| w == eqr));
    }

    // ───────────────────────────────────────────────────────────────────
    // T16 KATs — idle timeout 57014 query_canceled.
    //
    // We can't drive a real OS read-timeout in an in-memory pipe, so we
    // use a `Pipe`-like type that returns `WouldBlock` on the FIRST
    // post-handshake read. That's the same `std::io::ErrorKind` a real
    // `TcpStream::set_read_timeout` would surface on Linux; on Windows
    // it would surface as `TimedOut`. The `is_idle_timeout` classifier
    // matches BOTH per the platform-difference note in `std::io`.
    //
    // Integration tests (kesseldb-server pg_idle.rs) drive a real
    // `TcpListener` with `pg_idle_timeout` set to 100ms.
    // ───────────────────────────────────────────────────────────────────

    /// Test pipe that returns `WouldBlock` once the canned inbound
    /// stream is exhausted. Models the OS-level timeout a real
    /// `TcpStream::set_read_timeout` would surface.
    struct WouldBlockPipe {
        inbound: Cursor<Vec<u8>>,
        outbound: Vec<u8>,
    }
    impl Read for WouldBlockPipe {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.inbound.read(buf)?;
            if n == 0 {
                // Once the canned bytes are drained, every read returns
                // WouldBlock — simulates the OS-level read_timeout firing.
                Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "simulated idle read-timeout",
                ))
            } else {
                Ok(n)
            }
        }
    }
    impl Write for WouldBlockPipe {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.outbound.write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }

    /// Test pipe that returns `TimedOut` once exhausted — the Windows-
    /// platform equivalent of `WouldBlock`. Locks the cross-platform
    /// classifier `is_idle_timeout` against drift.
    struct TimedOutPipe {
        inbound: Cursor<Vec<u8>>,
        outbound: Vec<u8>,
    }
    impl Read for TimedOutPipe {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.inbound.read(buf)?;
            if n == 0 {
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "simulated Windows-platform idle read-timeout",
                ))
            } else {
                Ok(n)
            }
        }
    }
    impl Write for TimedOutPipe {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.outbound.write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }

    /// Test pipe that returns `ConnectionReset` once exhausted — the
    /// peer-RST shape. Locks that the timeout-vs-RST classification
    /// does NOT emit a 57014 frame on peer-reset (the write would
    /// fail anyway — emitting against a RST'd socket is wasted I/O
    /// and may flag a noisy log).
    struct ResetPipe {
        inbound: Cursor<Vec<u8>>,
        outbound: Vec<u8>,
    }
    impl Read for ResetPipe {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.inbound.read(buf)?;
            if n == 0 {
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "simulated peer RST",
                ))
            } else {
                Ok(n)
            }
        }
    }
    impl Write for ResetPipe {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.outbound.write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }

    /// HEADLINE T16 KAT: a successful handshake followed by an idle
    /// read returns `PgError::IdleTimeout` and the outbound bytes
    /// contain the FATAL `57014` query_canceled ErrorResponse before
    /// the session loop exits. libpq surfaces the SQLSTATE +
    /// message verbatim in `PQerrorMessage()`.
    #[test]
    fn t16_idle_timeout_emits_57014_fatal_before_close() {
        let token = b"kessel-bearer-token";
        // Only the handshake bytes — no Q/X after. Next read returns
        // WouldBlock (simulated OS-level read_timeout firing).
        let inbound = build_authed_inbound(
            token, "clientN", "serverN", "test", &[],
        );
        let mut pipe = WouldBlockPipe {
            inbound: Cursor::new(inbound),
            outbound: Vec::new(),
        };
        let engine = EmptySelectEngine;
        let r = run_session(
            &mut pipe,
            Some(token),
            || "serverN".to_string(),
            &engine,
        );
        assert!(
            matches!(r, Err(PgError::IdleTimeout)),
            "expected Err(IdleTimeout), got {r:?}"
        );
        // Outbound bytes contain SQLSTATE 57014.
        assert!(
            pipe.outbound.windows(5).any(|w| w == b"57014"),
            "outbound must contain SQLSTATE 57014"
        );
        // Outbound bytes contain FATAL severity.
        assert!(
            pipe.outbound.windows(b"FATAL".len()).any(|w| w == b"FATAL"),
            "outbound must contain FATAL severity"
        );
        // Outbound bytes contain the canonical PG message text.
        assert!(
            pipe.outbound
                .windows(crate::error::IDLE_TIMEOUT_MESSAGE.len())
                .any(|w| w == crate::error::IDLE_TIMEOUT_MESSAGE.as_bytes()),
            "outbound must contain canonical idle-timeout message text"
        );
    }

    /// T16: TimedOut (Windows-platform equivalent of WouldBlock)
    /// triggers the SAME 57014 emit path. Locks the cross-platform
    /// `is_idle_timeout` classifier against drift.
    #[test]
    fn t16_timed_out_kind_also_triggers_57014() {
        let token = b"kessel-bearer-token";
        let inbound = build_authed_inbound(
            token, "clientN", "serverN", "test", &[],
        );
        let mut pipe = TimedOutPipe {
            inbound: Cursor::new(inbound),
            outbound: Vec::new(),
        };
        let engine = EmptySelectEngine;
        let r = run_session(
            &mut pipe,
            Some(token),
            || "serverN".to_string(),
            &engine,
        );
        assert!(matches!(r, Err(PgError::IdleTimeout)));
        assert!(pipe.outbound.windows(5).any(|w| w == b"57014"));
    }

    /// T16: an active session (handshake + Q + Terminate before any
    /// idle read returns WouldBlock) does NOT emit a 57014 frame.
    /// Locks the "active connection doesn't trip the timeout" invariant.
    #[test]
    fn t16_active_session_does_not_emit_57014() {
        let token = b"kessel-bearer-token";
        let mut extra = Vec::new();
        extra.extend_from_slice(&build_q_frame("SELECT * FROM t"));
        extra.extend_from_slice(&build_x_frame());
        let inbound = build_authed_inbound(
            token, "clientN", "serverN", "test", &extra,
        );
        // WouldBlockPipe will return WouldBlock only AFTER the Terminate
        // — but Terminate causes a clean return BEFORE that, so the
        // session never observes the simulated timeout.
        let mut pipe = WouldBlockPipe {
            inbound: Cursor::new(inbound),
            outbound: Vec::new(),
        };
        let engine = EmptySelectEngine;
        let session = run_session(
            &mut pipe,
            Some(token),
            || "serverN".to_string(),
            &engine,
        )
        .expect("active session terminates cleanly");
        assert_eq!(session.user, "test");
        assert!(
            !pipe.outbound.windows(5).any(|w| w == b"57014"),
            "active session MUST NOT emit 57014 ErrorResponse"
        );
        assert!(
            !pipe.outbound.windows(b"FATAL".len()).any(|w| w == b"FATAL"),
            "active session MUST NOT emit FATAL severity"
        );
    }

    /// T16: a clean `Terminate` ('X') message followed by EOF (no more
    /// bytes) does NOT emit a 57014 frame — Terminate returns the
    /// session BEFORE the read-loop observes EOF or timeout. Locks
    /// the "clean Terminate is silent" invariant per PG §55.2.3.
    #[test]
    fn t16_clean_terminate_does_not_emit_57014() {
        let token = b"kessel-bearer-token";
        let inbound = build_authed_inbound(
            token, "clientN", "serverN", "test", &build_x_frame(),
        );
        let mut pipe = WouldBlockPipe {
            inbound: Cursor::new(inbound),
            outbound: Vec::new(),
        };
        let engine = EmptySelectEngine;
        let session = run_session(
            &mut pipe,
            Some(token),
            || "serverN".to_string(),
            &engine,
        )
        .expect("clean Terminate returns Ok");
        assert_eq!(session.user, "test");
        assert!(!pipe.outbound.windows(5).any(|w| w == b"57014"));
        // Reaching here proves clean return — no 57014 emit, no IdleTimeout.
    }

    /// T16: peer-RST mid-session does NOT emit a 57014 frame (the
    /// write would fail anyway — emitting against a RST'd socket is
    /// wasted I/O and would surface a misleading "FATAL 57014" log
    /// for a peer-crash case where the real cause is the peer). The
    /// session returns `Err(Io(ConnectionReset))` instead.
    #[test]
    fn t16_peer_reset_does_not_emit_57014() {
        let token = b"kessel-bearer-token";
        let inbound = build_authed_inbound(
            token, "clientN", "serverN", "test", &[],
        );
        let mut pipe = ResetPipe {
            inbound: Cursor::new(inbound),
            outbound: Vec::new(),
        };
        let engine = EmptySelectEngine;
        let r = run_session(
            &mut pipe,
            Some(token),
            || "serverN".to_string(),
            &engine,
        );
        match r {
            Err(PgError::Io(std::io::ErrorKind::ConnectionReset)) => {}
            other => panic!("expected Io(ConnectionReset), got {other:?}"),
        }
        assert!(
            !pipe.outbound.windows(5).any(|w| w == b"57014"),
            "peer-RST MUST NOT emit 57014 ErrorResponse"
        );
    }

    /// T16: clean EOF (peer cleanly closed the read half before
    /// sending the next message) does NOT emit a 57014 frame. The
    /// session returns `Ok(session)` per the existing T8 contract;
    /// 57014 is only for idle-timeout, not for peer-close.
    #[test]
    fn t16_clean_eof_does_not_emit_57014() {
        let token = b"kessel-bearer-token";
        // Only handshake — next read returns EOF (default Cursor
        // behavior — not WouldBlock).
        let inbound = build_authed_inbound(
            token, "clientN", "serverN", "test", &[],
        );
        let mut pipe = Pipe::new(inbound);
        let engine = EmptySelectEngine;
        let session = run_session(
            &mut pipe,
            Some(token),
            || "serverN".to_string(),
            &engine,
        )
        .expect("clean EOF returns Ok(session)");
        assert_eq!(session.user, "test");
        assert!(
            !pipe.outbound.windows(5).any(|w| w == b"57014"),
            "clean EOF MUST NOT emit 57014 ErrorResponse"
        );
    }

    /// T16: `is_idle_timeout` classifier matches both WouldBlock and
    /// TimedOut. Locks the cross-platform invariant explicitly so a
    /// future refactor of the classifier can't drift.
    #[test]
    fn t16_is_idle_timeout_classifier() {
        assert!(is_idle_timeout(std::io::ErrorKind::WouldBlock));
        assert!(is_idle_timeout(std::io::ErrorKind::TimedOut));
        // Negative cases — these MUST NOT trigger the idle-timeout path.
        assert!(!is_idle_timeout(std::io::ErrorKind::UnexpectedEof));
        assert!(!is_idle_timeout(std::io::ErrorKind::ConnectionReset));
        assert!(!is_idle_timeout(std::io::ErrorKind::BrokenPipe));
        assert!(!is_idle_timeout(std::io::ErrorKind::ConnectionAborted));
        assert!(!is_idle_timeout(std::io::ErrorKind::Other));
    }
}
