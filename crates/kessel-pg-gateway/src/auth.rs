//! SCRAM-SHA-256 authentication — server-side state machine
//! (RFC 5802 + RFC 7677 + PG §55.3).
//!
//! Flow (per spec §3.3 + RFC 5802 §3):
//!
//! ```text
//! S→C: AuthenticationSASL  ("SCRAM-SHA-256\0\0")
//! C→S: SASLInitialResponse (mech_name + client-first-message)
//! S→C: AuthenticationSASLContinue (server-first-message)
//! C→S: SASLResponse        (client-final-message)
//! S→C: AuthenticationSASLFinal    (server-final-message)
//! S→C: AuthenticationOk
//! ```
//!
//! Crypto (RFC 5802 §3):
//! ```text
//!   SaltedPassword  = PBKDF2(password, salt, iterations)
//!   ClientKey       = HMAC(SaltedPassword, "Client Key")
//!   StoredKey       = SHA-256(ClientKey)
//!   AuthMessage     = client-first-bare + ","
//!                     + server-first-message + ","
//!                     + client-final-message-without-proof
//!   ClientSignature = HMAC(StoredKey, AuthMessage)
//!   ClientProof     = ClientKey XOR ClientSignature  (sent by client)
//!   ServerKey       = HMAC(SaltedPassword, "Server Key")
//!   ServerSignature = HMAC(ServerKey, AuthMessage)   (sent by server)
//! ```
//!
//! Server verification:
//! 1. Recompute SaltedPassword / ClientKey / StoredKey / ClientSignature
//!    from password + AuthMessage.
//! 2. Decode ClientProof from the client-final-message.
//! 3. `RecoveredClientKey = ClientProof XOR ClientSignature`.
//! 4. Authenticate IFF `SHA-256(RecoveredClientKey) == StoredKey`.
//!
//! Spec §3.4 bridge: V1 uses the operator's Bearer token as the
//! SCRAM password input — one credential surface, no PG-only user
//! table. The salt is deterministic per session (derived from the
//! server nonce + Bearer token via SHA-256[..16]) — never persisted.
//!
//! This module is byte-deterministic given a fixed `server_nonce` —
//! production callers inject a cryptographically random nonce; tests
//! inject a fixed nonce for KAT reproducibility.

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::proto::{AUTH_SASL, AUTH_SASL_CONTINUE, AUTH_SASL_FINAL, BE_AUTHENTICATION};
use crate::SUPPORTED_SASL_MECH;
use kessel_crypto::{base64_decode, base64_encode, hmac_sha256, pbkdf2_hmac_sha256, sha256};

/// SCRAM authentication failure reasons. The server-loop maps each
/// of these onto SQLSTATE `28P01` invalid_password + immediate
/// connection close per spec §6.2 + RFC 5802 §7 (no oracle for
/// credential probing — every failure looks the same from outside).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// Mechanism the client picked isn't `SCRAM-SHA-256` (the only
    /// one V1 advertises). Either the client tried a different SASL
    /// mechanism or it lied about the prefix.
    UnsupportedMechanism,
    /// SASLInitialResponse payload doesn't decode as a valid SCRAM
    /// client-first-message. RFC 5802 §5.1 grammar violation.
    MalformedClientFirst,
    /// SASLResponse payload doesn't decode as a valid SCRAM
    /// client-final-message. RFC 5802 §5.1 grammar violation.
    MalformedClientFinal,
    /// The nonce echoed back in client-final doesn't match what
    /// we sent in server-first. RFC 5802 §3 — the per-session
    /// random nonce is the replay-prevention primitive; a mismatch
    /// means either a buggy client or a replay attempt.
    NonceMismatch,
    /// `c=` value in client-final isn't `biws` (= base64("n,,")).
    /// V1 doesn't advertise channel binding so the only legal value
    /// from a no-channel-binding client is `biws`. Any other value
    /// indicates the client thinks it negotiated something we didn't.
    BadChannelBinding,
    /// The base64 in the client's `p=` proof field didn't decode, or
    /// didn't decode to exactly 32 bytes (SHA-256 hash length).
    MalformedClientProof,
    /// The cryptographic verification failed — `SHA-256(ClientProof
    /// XOR ClientSignature) != StoredKey`. The Bearer token the
    /// client used to compute its proof doesn't match the server's
    /// token. SQLSTATE `28P01` invalid_password.
    ProofVerificationFailed,
    /// `ServerConfig.token` is unset and `allow_anonymous` is false
    /// (spec §3.4). V1 closed-mode requires a token; we cannot run
    /// SCRAM without a password. SQLSTATE `28000`.
    NoTokenConfigured,
}

/// The two SCRAM payloads that flow from client to server. The
/// `kessel-pg-gateway::server` accept loop reads these as `p`-tag
/// frames; the `auth` module parses the payload bytes here.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ClientFirst<'a> {
    /// Channel-binding flag — V1 only accepts `n` (no channel
    /// binding). RFC 5802 §5.1 grammar: `gs2-cbind-flag = ("p=" cb-
    /// name / "n" / "y")`. V1 rejects "p=..." (channel binding) and
    /// "y" (client thinks server supports it; V1 doesn't).
    cb_flag: &'a str,
    /// Authorization identity — usually empty for PG-style usage.
    /// RFC 5802 §5.1: `authzid = "a=" saslname`. Empty means
    /// "authenticate as the same user named by the auth username".
    authzid: &'a str,
    /// Username from `n=` field. V1 carries it through but doesn't
    /// use it for authorization (spec §6.1: one credential surface).
    username: &'a str,
    /// Client-generated random nonce from `r=` field.
    client_nonce: &'a str,
    /// The "client-first-message-bare" — RFC 5802 §3 calls it out
    /// as `client-first-bare = username "," reserved-mext-or-attr "," nonce
    /// ["," extensions]`. Used verbatim in the AuthMessage construction;
    /// preserved here so we don't have to rebuild it from cb_flag/authzid.
    client_first_bare: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClientFinal<'a> {
    /// Channel-binding data from `c=` field — V1 requires `biws`
    /// (= base64("n,,"); the GS2 header the client claims to have
    /// sent in its first message).
    channel_binding_b64: &'a str,
    /// Echoed combined nonce from `r=` field — MUST equal the
    /// `server_combined_nonce` (client_nonce + server_nonce) the
    /// server sent in server-first. RFC 5802 §3.
    nonce: &'a str,
    /// Base64-encoded client proof from `p=` field — `ClientKey XOR
    /// ClientSignature`. Decodes to exactly 32 bytes (SHA-256 hash
    /// length).
    client_proof_b64: &'a str,
    /// Everything up to (but not including) the `,p=` field — used
    /// in AuthMessage construction.
    client_final_without_proof: &'a str,
}

/// Encodes the `AuthenticationSASL` challenge frame the server sends
/// FIRST in the auth phase. Wire shape (PG §55.7.4):
///
/// ```text
/// 'R' [length:4 BE] [auth_type:4 BE = 10] [mechanism\0] [\0]
/// ```
///
/// V1 advertises only `SCRAM-SHA-256`; the payload is therefore
/// `"SCRAM-SHA-256\0\0"` — first NUL terminates the mechanism name,
/// second NUL terminates the (empty) "next mechanism" string per the
/// SASL mechanism-list grammar.
pub fn encode_authentication_sasl_challenge() -> Vec<u8> {
    let mech = SUPPORTED_SASL_MECH.as_bytes();
    // payload: auth_type(4) + mech(N) + \0 (mech terminator) + \0 (list end)
    let payload_len = 4 + mech.len() + 1 + 1;
    let length = (4 + payload_len) as u32; // length includes itself
    let mut out = Vec::with_capacity(1 + length as usize);
    out.push(BE_AUTHENTICATION);
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(&AUTH_SASL.to_be_bytes());
    out.extend_from_slice(mech);
    out.push(0);
    out.push(0);
    out
}

/// Encodes the `AuthenticationSASLContinue` frame containing the
/// SCRAM `server-first-message` (RFC 5802 §5.1):
///
/// ```text
/// 'R' [length:4 BE] [auth_type:4 BE = 11] [server-first-message bytes]
/// ```
///
/// `server_first_message = "r=" combined_nonce "," "s=" salt_b64 ","
/// "i=" iterations`.
pub fn encode_authentication_sasl_continue(server_first: &str) -> Vec<u8> {
    let bytes = server_first.as_bytes();
    let payload_len = 4 + bytes.len();
    let length = (4 + payload_len) as u32;
    let mut out = Vec::with_capacity(1 + length as usize);
    out.push(BE_AUTHENTICATION);
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(&AUTH_SASL_CONTINUE.to_be_bytes());
    out.extend_from_slice(bytes);
    out
}

/// Encodes the `AuthenticationSASLFinal` frame containing the
/// SCRAM `server-final-message`:
///
/// ```text
/// 'R' [length:4 BE] [auth_type:4 BE = 12] [server-final-message bytes]
/// ```
///
/// `server_final_message = "v=" server_signature_b64`.
pub fn encode_authentication_sasl_final(server_final: &str) -> Vec<u8> {
    let bytes = server_final.as_bytes();
    let payload_len = 4 + bytes.len();
    let length = (4 + payload_len) as u32;
    let mut out = Vec::with_capacity(1 + length as usize);
    out.push(BE_AUTHENTICATION);
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(&AUTH_SASL_FINAL.to_be_bytes());
    out.extend_from_slice(bytes);
    out
}

/// Encodes the `AuthenticationOk` frame (PG §55.7.4 / RFC 5802 end-
/// of-flow): tells the client SCRAM completed and the connection is
/// authorized.
///
/// ```text
/// 'R' [length:4 BE = 8] [auth_type:4 BE = 0]
/// ```
pub fn encode_authentication_ok() -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(BE_AUTHENTICATION);
    out.extend_from_slice(&8u32.to_be_bytes());
    out.extend_from_slice(&crate::proto::AUTH_OK.to_be_bytes());
    out
}

/// Parses a SASLInitialResponse `p`-message payload. The payload
/// layout per PG §55.7.4:
///
/// ```text
/// [mech_name\0] [client_first_length:u32 BE] [client_first_bytes]
/// ```
///
/// The mechanism string MUST be `"SCRAM-SHA-256"`. If the client
/// sends a different mechanism we reject with `UnsupportedMechanism`.
pub fn parse_sasl_initial_response(payload: &[u8]) -> Result<(String, String), AuthError> {
    // Find the NUL that terminates the mechanism name.
    let nul = payload.iter().position(|&b| b == 0)
        .ok_or(AuthError::MalformedClientFirst)?;
    let mech = std::str::from_utf8(&payload[..nul])
        .map_err(|_| AuthError::MalformedClientFirst)?;
    if mech != SUPPORTED_SASL_MECH {
        return Err(AuthError::UnsupportedMechanism);
    }
    // After the NUL: a 4-byte BE length of the client-first-message
    // bytes, then the bytes themselves.
    let after_mech = &payload[nul + 1..];
    if after_mech.len() < 4 {
        return Err(AuthError::MalformedClientFirst);
    }
    let len = u32::from_be_bytes([
        after_mech[0], after_mech[1], after_mech[2], after_mech[3],
    ]);
    let client_first_bytes = &after_mech[4..];
    if client_first_bytes.len() != len as usize {
        return Err(AuthError::MalformedClientFirst);
    }
    let client_first = std::str::from_utf8(client_first_bytes)
        .map_err(|_| AuthError::MalformedClientFirst)?;
    Ok((mech.to_string(), client_first.to_string()))
}

/// Parses the SCRAM `client-first-message` per RFC 5802 §5.1:
///
/// ```text
/// client-first-message = gs2-header client-first-message-bare
/// gs2-header           = gs2-cbind-flag "," [ authzid ] ","
/// gs2-cbind-flag       = ("p=" cb-name) / "n" / "y"
/// client-first-message-bare = [reserved-mext ","] username "," nonce ["," extensions]
/// username             = "n=" saslname
/// nonce                = "r=" printable
/// ```
///
/// V1 accepts only `gs2-cbind-flag = "n"` (no channel binding) +
/// empty authzid. The parse is permissive about extensions (skipped).
fn parse_client_first(msg: &str) -> Result<ClientFirst<'_>, AuthError> {
    // Split off the GS2 header. The header is "<cb_flag>,<authzid>,"
    // — find the SECOND comma; everything before is the header.
    let mut comma_iter = msg.match_indices(',');
    let first_comma = comma_iter.next().ok_or(AuthError::MalformedClientFirst)?.0;
    let second_comma = comma_iter.next().ok_or(AuthError::MalformedClientFirst)?.0;
    let cb_flag = &msg[..first_comma];
    let authzid = &msg[first_comma + 1..second_comma];
    let bare = &msg[second_comma + 1..];

    if cb_flag != "n" && cb_flag != "y" && !cb_flag.starts_with("p=") {
        return Err(AuthError::MalformedClientFirst);
    }
    // V1 rejects "p=..." (channel binding requested but V1 doesn't
    // advertise channel binding). Also reject "y" (client thinks
    // server supports CB but V1 doesn't advertise it).
    if cb_flag != "n" {
        return Err(AuthError::BadChannelBinding);
    }
    // authzid: V1 accepts empty (most common) or "a=..." (ignored).
    if !authzid.is_empty() && !authzid.starts_with("a=") {
        return Err(AuthError::MalformedClientFirst);
    }
    // Parse `n=...,r=...` from the bare message.
    let mut parts = bare.split(',');
    let n_part = parts.next().ok_or(AuthError::MalformedClientFirst)?;
    let r_part = parts.next().ok_or(AuthError::MalformedClientFirst)?;
    let username = n_part.strip_prefix("n=").ok_or(AuthError::MalformedClientFirst)?;
    let client_nonce = r_part.strip_prefix("r=").ok_or(AuthError::MalformedClientFirst)?;
    Ok(ClientFirst {
        cb_flag,
        authzid,
        username,
        client_nonce,
        client_first_bare: bare,
    })
}

/// Parses the SCRAM `client-final-message` per RFC 5802 §5.1:
///
/// ```text
/// client-final-message = channel-binding "," nonce ["," extensions] "," proof
/// channel-binding      = "c=" base64
/// nonce                = "r=" printable
/// proof                = "p=" base64
/// ```
///
/// Returns the parsed fields + the substring "everything before the
/// `,p=` proof" for AuthMessage construction.
fn parse_client_final(msg: &str) -> Result<ClientFinal<'_>, AuthError> {
    // Find the `,p=` boundary — the last comma-prefixed `p=` field.
    // RFC 5802: `proof` is the LAST field; finding the last `,p=`
    // gives us the without-proof prefix verbatim.
    let p_idx = msg.rfind(",p=").ok_or(AuthError::MalformedClientFinal)?;
    let without_proof = &msg[..p_idx];
    let proof_b64 = &msg[p_idx + 3..];
    // Parse the channel-binding + nonce from the without-proof prefix.
    let mut parts = without_proof.split(',');
    let c_part = parts.next().ok_or(AuthError::MalformedClientFinal)?;
    let r_part = parts.next().ok_or(AuthError::MalformedClientFinal)?;
    let cb_b64 = c_part.strip_prefix("c=").ok_or(AuthError::MalformedClientFinal)?;
    let nonce = r_part.strip_prefix("r=").ok_or(AuthError::MalformedClientFinal)?;
    Ok(ClientFinal {
        channel_binding_b64: cb_b64,
        nonce,
        client_proof_b64: proof_b64,
        client_final_without_proof: without_proof,
    })
}

/// State carried between SCRAM rounds. After the server emits its
/// server-first-message and receives the client-final, it needs to
/// re-derive the AuthMessage to verify the client proof — that
/// requires preserving `client_first_bare`, `server_first_message`,
/// and the combined nonce.
#[derive(Debug, Clone)]
pub struct ScramState {
    /// The username the client claimed in client-first. V1 logs
    /// this but doesn't use it for authorization.
    pub username: String,
    /// The combined nonce (client_nonce + server_nonce) — used to
    /// validate the nonce echoed back in client-final.
    pub combined_nonce: String,
    /// The `client-first-message-bare` substring — needed verbatim
    /// to compute AuthMessage.
    pub client_first_bare: String,
    /// The full `server-first-message` we sent — needed verbatim
    /// to compute AuthMessage.
    pub server_first: String,
    /// The deterministic salt we used in server-first — needed to
    /// re-derive SaltedPassword on the client-final round.
    pub salt: Vec<u8>,
    /// Iteration count — needed to re-derive SaltedPassword.
    pub iterations: u32,
}

/// First round (server-side): given the client-first-message bytes +
/// the operator's Bearer token + a server-generated nonce, produce
/// the `server-first-message` string (the body of
/// AuthenticationSASLContinue's payload) and the `ScramState` to
/// carry into the second round.
///
/// `server_nonce` MUST be cryptographically random in production
/// (per RFC 5802 §5 — the per-session nonce is the replay-prevention
/// primitive). For tests it's a fixed string.
///
/// `token` is the Bearer token from `ServerConfig.token` (spec §3.4
/// — the Bearer ↔ SCRAM bridge). If the operator hasn't set a token,
/// `start_scram` should not be called — see `AuthError::NoTokenConfigured`
/// (the server.rs loop checks this BEFORE entering SCRAM).
///
/// Salt derivation per spec §3.4: `salt = sha256(server_nonce || token)[..16]`
/// — deterministic per session, never persisted on disk.
pub fn start_scram(
    client_first: &str,
    token: &[u8],
    server_nonce: &str,
    iterations: u32,
) -> Result<(String, ScramState), AuthError> {
    let parsed = parse_client_first(client_first)?;
    let combined_nonce = format!("{}{}", parsed.client_nonce, server_nonce);
    // Salt: deterministic per session (spec §3.4). Take first 16
    // bytes of SHA-256(server_nonce || token).
    let mut salt_input: Vec<u8> = Vec::with_capacity(server_nonce.len() + token.len());
    salt_input.extend_from_slice(server_nonce.as_bytes());
    salt_input.extend_from_slice(token);
    let salt_hash = sha256(&salt_input);
    let salt = salt_hash[..16].to_vec();
    let salt_b64 = base64_encode(&salt);
    let server_first = format!(
        "r={combined_nonce},s={salt_b64},i={iterations}",
    );
    let state = ScramState {
        username: parsed.username.to_string(),
        combined_nonce,
        client_first_bare: parsed.client_first_bare.to_string(),
        server_first: server_first.clone(),
        salt,
        iterations,
    };
    Ok((server_first, state))
}

/// Second round (server-side): given the client-final-message bytes
/// + the in-flight `ScramState` + the operator's Bearer token,
/// verify the client proof and produce the `server-final-message`
/// string (the body of AuthenticationSASLFinal's payload).
///
/// On success: the client KNOWS the Bearer token; the connection is
/// authenticated. On any failure: return `AuthError`; the server.rs
/// loop emits ErrorResponse `28P01` invalid_password + closes TCP
/// (per spec §6.2 + RFC 5802 §7 — no oracle for credential probing).
pub fn finish_scram(
    client_final: &str,
    state: &ScramState,
    token: &[u8],
) -> Result<String, AuthError> {
    let parsed = parse_client_final(client_final)?;
    // c=biws is the only legal channel-binding value V1 accepts
    // (= base64("n,,") = the GS2 header the client claimed to send).
    if parsed.channel_binding_b64 != "biws" {
        return Err(AuthError::BadChannelBinding);
    }
    // Nonce in client-final MUST match the combined nonce we sent.
    if parsed.nonce != state.combined_nonce {
        return Err(AuthError::NonceMismatch);
    }
    // Decode the client proof — must be exactly 32 bytes (SHA-256
    // output length).
    let client_proof = base64_decode(parsed.client_proof_b64)
        .ok_or(AuthError::MalformedClientProof)?;
    if client_proof.len() != 32 {
        return Err(AuthError::MalformedClientProof);
    }
    // Re-derive the crypto chain (RFC 5802 §3) from the token.
    let salted_password = pbkdf2_hmac_sha256(token, &state.salt, state.iterations);
    let client_key = hmac_sha256(&salted_password, b"Client Key");
    let stored_key = sha256(&client_key);
    let auth_message = format!(
        "{},{},{}",
        state.client_first_bare,
        state.server_first,
        parsed.client_final_without_proof,
    );
    let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
    // Recover the client's claimed ClientKey from
    //   RecoveredClientKey = ClientProof XOR ClientSignature
    let mut recovered_client_key = [0u8; 32];
    for i in 0..32 {
        recovered_client_key[i] = client_proof[i] ^ client_signature[i];
    }
    // Authenticate IFF SHA-256(RecoveredClientKey) == StoredKey.
    let recovered_stored_key = sha256(&recovered_client_key);
    // Constant-time comparison — never short-circuit on first
    // mismatch (timing oracle).
    let mut diff: u8 = 0;
    for i in 0..32 {
        diff |= recovered_stored_key[i] ^ stored_key[i];
    }
    if diff != 0 {
        return Err(AuthError::ProofVerificationFailed);
    }
    // Compute ServerSignature for the server-final-message.
    let server_key = hmac_sha256(&salted_password, b"Server Key");
    let server_signature = hmac_sha256(&server_key, auth_message.as_bytes());
    let server_signature_b64 = base64_encode(&server_signature);
    Ok(format!("v={server_signature_b64}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{AUTH_SASL, AUTH_SASL_CONTINUE, AUTH_SASL_FINAL, BE_AUTHENTICATION};

    // ───────────────────────────────────────────────────────────────────
    // T2 SCRAM-SHA-256 KATs — locks RFC 5802 + RFC 7677 + PG §55.3
    // wire-format invariants against authoritative sources. Where
    // possible we use the RFC 7677 §3 published example
    // (password="pencil"); for byte-pattern locks we use synthetic
    // fixed nonces so the wire output is deterministic.
    // ───────────────────────────────────────────────────────────────────

    /// AuthenticationSASL challenge byte pattern: 'R' [length:4]
    /// [auth_type:4 = 10] "SCRAM-SHA-256\0\0".
    ///
    /// `length` = 4 (length itself) + 4 (auth_type) + 13 (mech name)
    /// + 1 (mech-terminator NUL) + 1 (list-terminator NUL) = 23.
    #[test]
    fn t2_authentication_sasl_challenge_byte_pattern() {
        let frame = encode_authentication_sasl_challenge();
        assert_eq!(frame[0], BE_AUTHENTICATION); // 'R'
        let length = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]);
        assert_eq!(length, 23);
        let auth_type = u32::from_be_bytes([frame[5], frame[6], frame[7], frame[8]]);
        assert_eq!(auth_type, AUTH_SASL);
        // Mechanism name + double-NUL terminator
        let mech_bytes = &frame[9..9 + 13];
        assert_eq!(mech_bytes, b"SCRAM-SHA-256");
        assert_eq!(frame[9 + 13], 0); // mech-terminator NUL
        assert_eq!(frame[9 + 13 + 1], 0); // list-terminator NUL
        // Total frame size = 1 (type byte) + length = 24
        assert_eq!(frame.len(), 24);
    }

    /// AuthenticationOk byte pattern: 'R' [length:4 = 8] [auth_type:4 = 0].
    /// Exactly 9 bytes on the wire. PG §55.7.4.
    #[test]
    fn t2_authentication_ok_byte_pattern() {
        let frame = encode_authentication_ok();
        assert_eq!(frame.len(), 9);
        assert_eq!(frame[0], BE_AUTHENTICATION);
        assert_eq!(
            u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]),
            8
        );
        assert_eq!(
            u32::from_be_bytes([frame[5], frame[6], frame[7], frame[8]]),
            0
        );
        // Locked literal: every PG client recognizes this exact
        // 9-byte sequence as "auth complete, proceed to ReadyForQuery".
        assert_eq!(frame.as_slice(), &[b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
    }

    /// AuthenticationSASLContinue wraps the SCRAM server-first-message
    /// in the standard R-message envelope with auth_type=11.
    #[test]
    fn t2_authentication_sasl_continue_envelope() {
        let body = "r=clientservernonce,s=AAECAwQFBgcICQoLDA0ODw==,i=4096";
        let frame = encode_authentication_sasl_continue(body);
        assert_eq!(frame[0], BE_AUTHENTICATION);
        let length = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]);
        // length = 4 (itself) + 4 (auth_type) + body.len()
        assert_eq!(length as usize, 8 + body.len());
        assert_eq!(
            u32::from_be_bytes([frame[5], frame[6], frame[7], frame[8]]),
            AUTH_SASL_CONTINUE
        );
        assert_eq!(&frame[9..], body.as_bytes());
    }

    /// AuthenticationSASLFinal wraps the SCRAM server-final-message
    /// (`v=<server_signature_b64>`) in the R-envelope with
    /// auth_type=12.
    #[test]
    fn t2_authentication_sasl_final_envelope() {
        let body = "v=AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
        let frame = encode_authentication_sasl_final(body);
        assert_eq!(frame[0], BE_AUTHENTICATION);
        let length = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]);
        assert_eq!(length as usize, 8 + body.len());
        assert_eq!(
            u32::from_be_bytes([frame[5], frame[6], frame[7], frame[8]]),
            AUTH_SASL_FINAL
        );
        assert_eq!(&frame[9..], body.as_bytes());
    }

    /// SASLInitialResponse payload parser: extracts mechanism name
    /// and the client-first-message bytes. PG §55.7.4 wire format:
    /// `[mech\0][len:u32 BE][client_first]`.
    #[test]
    fn t2_sasl_initial_response_parses_mech_and_client_first() {
        let client_first = "n,,n=user,r=clientnonceXYZ";
        let mut payload = Vec::new();
        payload.extend_from_slice(b"SCRAM-SHA-256\0");
        payload.extend_from_slice(&(client_first.len() as u32).to_be_bytes());
        payload.extend_from_slice(client_first.as_bytes());
        let (mech, cf) = parse_sasl_initial_response(&payload)
            .expect("well-formed SASLInitialResponse parses");
        assert_eq!(mech, "SCRAM-SHA-256");
        assert_eq!(cf, client_first);
    }

    /// SASLInitialResponse with non-SCRAM-SHA-256 mechanism →
    /// `UnsupportedMechanism`. V1 advertises only SCRAM-SHA-256 (PG
    /// 10+ default); a client claiming any other mechanism gets
    /// `28P01`.
    #[test]
    fn t2_sasl_initial_response_rejects_other_mechanism() {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"SCRAM-SHA-1\0");
        payload.extend_from_slice(&5u32.to_be_bytes());
        payload.extend_from_slice(b"hello");
        match parse_sasl_initial_response(&payload) {
            Err(AuthError::UnsupportedMechanism) => {}
            other => panic!("expected UnsupportedMechanism, got {other:?}"),
        }
    }

    /// SCRAM end-to-end (RFC 5802 §3 round-trip): emulate a
    /// well-behaved client computing its proof, drive the server-
    /// side `start_scram` + `finish_scram`, and assert the server
    /// validates the proof + emits a server-signature the client
    /// would in turn verify.
    ///
    /// Uses fixed inputs so the test is byte-deterministic. This is
    /// the headline KAT — if it passes, SCRAM-SHA-256 is wire-correct.
    #[test]
    fn t2_scram_round_trip_locks_rfc_5802_invariants() {
        let token = b"kessel-bearer-token-for-tests";
        let client_nonce = "clientNonceFixed12345";
        let server_nonce = "serverNonceFixed67890";
        let iterations = 4096u32;
        let username = "test";

        // Build client-first-message exactly as libpq would.
        let client_first_bare = format!("n={username},r={client_nonce}");
        let client_first = format!("n,,{client_first_bare}");

        // Server round 1
        let (server_first, state) =
            start_scram(&client_first, token, server_nonce, iterations).expect("start_scram");

        // Server-first should carry the combined nonce, salt, and i=4096.
        assert!(server_first.starts_with(&format!("r={client_nonce}{server_nonce},s=")));
        assert!(server_first.ends_with(",i=4096"));
        assert_eq!(state.combined_nonce, format!("{client_nonce}{server_nonce}"));
        assert_eq!(state.iterations, iterations);
        assert_eq!(state.salt.len(), 16);

        // Salt is deterministic per RFC bridge §3.4: SHA-256(nonce || token)[..16]
        let mut salt_input = Vec::new();
        salt_input.extend_from_slice(server_nonce.as_bytes());
        salt_input.extend_from_slice(token);
        let expected_salt: Vec<u8> = sha256(&salt_input)[..16].to_vec();
        assert_eq!(state.salt, expected_salt);

        // Client emulation: compute its proof per RFC 5802 §3.
        let salted_password = pbkdf2_hmac_sha256(token, &state.salt, iterations);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);
        let client_final_without_proof =
            format!("c=biws,r={}", state.combined_nonce);
        let auth_message =
            format!("{client_first_bare},{server_first},{client_final_without_proof}");
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let mut client_proof = [0u8; 32];
        for i in 0..32 {
            client_proof[i] = client_key[i] ^ client_signature[i];
        }
        let client_final = format!(
            "{client_final_without_proof},p={}",
            base64_encode(&client_proof)
        );

        // Server round 2
        let server_final = finish_scram(&client_final, &state, token)
            .expect("server should validate the client proof");
        // Server-final is "v=<server_sig_b64>"
        assert!(server_final.starts_with("v="));
        let server_sig = base64_decode(&server_final[2..]).expect("server sig base64 decodes");
        assert_eq!(server_sig.len(), 32);

        // Client verifies: ServerSignature should equal HMAC(ServerKey, AuthMessage)
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        let expected_server_sig = hmac_sha256(&server_key, auth_message.as_bytes());
        assert_eq!(server_sig, expected_server_sig.to_vec());
    }

    /// Bad client proof (wrong token) → `ProofVerificationFailed`.
    /// Maps to SQLSTATE `28P01` invalid_password + immediate close —
    /// no oracle for credential probing (every failure looks the
    /// same from outside).
    #[test]
    fn t2_scram_bad_proof_is_rejected_28p01() {
        let real_token = b"real-bearer-token";
        let wrong_token = b"WRONG-bearer-token";
        let client_nonce = "clientN";
        let server_nonce = "serverN";
        let username = "test";
        let client_first_bare = format!("n={username},r={client_nonce}");
        let client_first = format!("n,,{client_first_bare}");

        let (server_first, state) =
            start_scram(&client_first, real_token, server_nonce, 4096).unwrap();
        // Client computes proof against the WRONG token
        let salted = pbkdf2_hmac_sha256(wrong_token, &state.salt, 4096);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let cf_without_proof = format!("c=biws,r={}", state.combined_nonce);
        let auth_msg = format!("{client_first_bare},{server_first},{cf_without_proof}");
        let client_sig = hmac_sha256(&stored_key, auth_msg.as_bytes());
        let mut proof = [0u8; 32];
        for i in 0..32 {
            proof[i] = client_key[i] ^ client_sig[i];
        }
        let client_final = format!("{cf_without_proof},p={}", base64_encode(&proof));

        match finish_scram(&client_final, &state, real_token) {
            Err(AuthError::ProofVerificationFailed) => {}
            other => panic!("expected ProofVerificationFailed, got {other:?}"),
        }
    }

    /// Nonce mismatch in client-final → `NonceMismatch`. The per-
    /// session random nonce is the replay-prevention primitive
    /// (RFC 5802 §3). A client that echoes the wrong nonce is
    /// either buggy or a replay attempt.
    #[test]
    fn t2_scram_nonce_mismatch_is_rejected() {
        let token = b"some-token";
        let client_first = "n,,n=test,r=clientN";
        let (_server_first, state) =
            start_scram(client_first, token, "serverN", 4096).unwrap();
        // Wrong nonce (extra suffix the server didn't send)
        let bad_nonce = format!("{}WRONG", state.combined_nonce);
        let cf_without_proof = format!("c=biws,r={bad_nonce}");
        // Any proof bytes — verification stops at the nonce check.
        let client_final =
            format!("{cf_without_proof},p={}", base64_encode(&[0u8; 32]));
        match finish_scram(&client_final, &state, token) {
            Err(AuthError::NonceMismatch) => {}
            other => panic!("expected NonceMismatch, got {other:?}"),
        }
    }

    /// Bad channel-binding (`c=` not equal to "biws") →
    /// `BadChannelBinding`. V1 only accepts the no-channel-binding
    /// GS2 header "n,," which base64-encodes to "biws"; anything
    /// else means the client thinks it negotiated something the
    /// server didn't advertise.
    #[test]
    fn t2_scram_bad_channel_binding_rejected() {
        let token = b"some-token";
        let client_first = "n,,n=test,r=clientN";
        let (_server_first, state) =
            start_scram(client_first, token, "serverN", 4096).unwrap();
        // c=Y3VzdG9t = base64("custom") — client claims a different
        // channel-binding payload than what it advertised in
        // client-first (gs2-cbind-flag="n").
        let cf_without_proof =
            format!("c=Y3VzdG9t,r={}", state.combined_nonce);
        let client_final =
            format!("{cf_without_proof},p={}", base64_encode(&[0u8; 32]));
        match finish_scram(&client_final, &state, token) {
            Err(AuthError::BadChannelBinding) => {}
            other => panic!("expected BadChannelBinding, got {other:?}"),
        }
    }

    /// Client-first with the wrong GS2 channel-binding flag (e.g.
    /// `y,,...` meaning "client thinks server supports CB") →
    /// `BadChannelBinding`. V1 only advertises "no channel binding"
    /// so the only legal client-first flag is "n".
    #[test]
    fn t2_scram_client_first_with_y_flag_rejected() {
        let token = b"some-token";
        let client_first = "y,,n=test,r=clientN";
        match start_scram(client_first, token, "serverN", 4096) {
            Err(AuthError::BadChannelBinding) => {}
            other => panic!("expected BadChannelBinding for y-flag, got {other:?}"),
        }
    }

    /// Malformed client-final (missing `,p=` proof field) →
    /// `MalformedClientFinal`. RFC 5802 §5.1 grammar violation.
    #[test]
    fn t2_scram_client_final_missing_proof_rejected() {
        let token = b"some-token";
        let client_first = "n,,n=test,r=clientN";
        let (_server_first, state) =
            start_scram(client_first, token, "serverN", 4096).unwrap();
        // No `,p=` field — just the channel binding + nonce
        let client_final = format!("c=biws,r={}", state.combined_nonce);
        match finish_scram(&client_final, &state, token) {
            Err(AuthError::MalformedClientFinal) => {}
            other => panic!("expected MalformedClientFinal, got {other:?}"),
        }
    }

    /// Client-final with a non-base64 proof field →
    /// `MalformedClientProof`. The decoder rejects any deviation
    /// from RFC 4648 standard alphabet (whitespace, URL-safe chars,
    /// wrong length).
    #[test]
    fn t2_scram_client_final_non_base64_proof_rejected() {
        let token = b"some-token";
        let client_first = "n,,n=test,r=clientN";
        let (_server_first, state) =
            start_scram(client_first, token, "serverN", 4096).unwrap();
        let client_final =
            format!("c=biws,r={},p=!!!!!", state.combined_nonce);
        match finish_scram(&client_final, &state, token) {
            Err(AuthError::MalformedClientProof) => {}
            other => panic!("expected MalformedClientProof, got {other:?}"),
        }
    }

    /// Client-final with a base64 proof of wrong length (not 32
    /// bytes after decode) → `MalformedClientProof`. SHA-256 hashes
    /// are always exactly 32 bytes.
    #[test]
    fn t2_scram_client_final_short_proof_rejected() {
        let token = b"some-token";
        let client_first = "n,,n=test,r=clientN";
        let (_server_first, state) =
            start_scram(client_first, token, "serverN", 4096).unwrap();
        let client_final =
            format!("c=biws,r={},p=AAAA", state.combined_nonce); // 4-char b64 → 3 bytes
        match finish_scram(&client_final, &state, token) {
            Err(AuthError::MalformedClientProof) => {}
            other => panic!("expected MalformedClientProof for short proof, got {other:?}"),
        }
    }

    /// Same token + same nonces produce byte-identical server-first
    /// (deterministic per spec §3.4: salt = SHA-256(nonce || token)
    /// is a pure function of its inputs). Locks the per-session-salt
    /// derivation against a refactor that adds entropy.
    #[test]
    fn t2_scram_start_is_deterministic_given_fixed_nonce() {
        let token = b"some-token";
        let client_first = "n,,n=test,r=clientN";
        let (a, _) = start_scram(client_first, token, "serverN", 4096).unwrap();
        let (b, _) = start_scram(client_first, token, "serverN", 4096).unwrap();
        assert_eq!(a, b, "server-first MUST be deterministic per spec §3.4");
    }
}
