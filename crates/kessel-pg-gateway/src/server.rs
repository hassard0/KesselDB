//! PG-gateway listener + per-connection accept stub.
//!
//! **T1 status (current):** scaffold only. `accept()` returns
//! `Err(PgError::NotYetImplemented)` without touching the stream. This
//! locks the surface area + signature so T2's startup-handshake
//! implementation can flip the stub in-place without churning the
//! caller (mirrors the SP-WS T1 → T2 pattern where
//! `ws::handle_upgrade` started as a placeholder and T2 replaced its
//! body with a real handshake).
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`

#![forbid(unsafe_code)]
#![allow(dead_code)]

use std::io::Write;

/// Errors a PG-wire session can return at any phase.
///
/// **T1 status:** only `NotYetImplemented` exists. T2 widens this with
/// `StartupFailed(SqlState)`, `AuthFailed`, `ProtocolViolation(SqlState)`,
/// `Io(ErrorKind)`. Until T2 flips the stub, ANY call to `accept()`
/// returns `NotYetImplemented`; the T1 KAT below locks that surface.
#[derive(Debug)]
pub enum PgError {
    /// T1 placeholder. T2 removes this in favor of real error
    /// variants. The associated KAT in this module is the
    /// regression-lock — flipping the stub MUST update that test.
    NotYetImplemented,
}

/// Per-connection accept entry point. Called by the listener after
/// `TcpStream::accept()` succeeds; owns the stream for the lifetime
/// of the connection.
///
/// **T1 stub:** returns `Err(PgError::NotYetImplemented)` without
/// reading or writing the stream. T2 replaces with the real startup-
/// handshake + SCRAM-SHA-256 auth + ReadyForQuery emit. T3+ extends
/// to the simple-query loop. The `_stream` argument is held by name
/// so the signature is stable across T1 → T2 → T3+ transitions.
///
/// Generic over `Write` (the loosest bound T2 needs — T2 only writes
/// the auth challenge + error responses). T5+ session loop widens
/// back to `Read + Write` per spec §8.5 (mirror SP-WS T2's "narrow,
/// then widen at the session-loop slice" pattern).
pub fn accept<S: Write>(_stream: &mut S) -> Result<(), PgError> {
    Err(PgError::NotYetImplemented)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T1 stub regression-lock. Mirrors the SP-WS T1
    /// `t1_handle_upgrade_returns_not_yet_implemented_stub` shape:
    /// T2 MUST update this test alongside the real handshake response
    /// — flipping the stub is the gate that catches a half-shipped T2.
    #[test]
    fn t1_accept_returns_not_yet_implemented_stub() {
        let mut sink: Vec<u8> = Vec::new();
        match accept(&mut sink) {
            Err(PgError::NotYetImplemented) => {}
            other => panic!(
                "expected PgError::NotYetImplemented; got {:?}; \
                 if T2 has flipped the stub, update this regression-lock \
                 alongside the new behavior",
                other
            ),
        }
        // The stub MUST NOT write anything to the stream before
        // returning. T2's real implementation WILL write — the new
        // test will assert against the expected bytes (e.g.
        // AuthenticationSASL challenge).
        assert_eq!(
            sink.len(),
            0,
            "T1 stub must not touch the stream; T2 will write the \
             AuthenticationSASL challenge"
        );
    }
}
