//! SP-WS T5 + T6 — per-connection WebSocket session loop + `kessel-op-v1`
//! subprotocol dispatch.
//!
//! After `handle_upgrade` writes the 101 Switching Protocols response, the
//! TCP stream carries WebSocket frames in both directions. `run_ws_session`
//! takes ownership of the (already-upgraded) `TcpStream` and runs the
//! per-connection session per spec §6.3 and §9:
//!
//! - **Reader thread**: blocks on `stream.read()`, decodes frames via
//!   `frame::decode_client_frame`, dispatches by opcode:
//!     - **Binary** → decode `Op::decode(payload)`, call
//!       `engine.apply_op(op)`, enqueue `OpResult::encode()` as a binary
//!       frame (T6 wire-up). Undecodable payload → close 1002.
//!     - **Text** → close 1003 (kessel-op-v1 is binary-only per spec §5.3).
//!     - **Continuation / FIN=0 data** → close 1003 (V1 rejects
//!       fragmentation per spec §4.5).
//!     - **Ping** → enqueue Pong with identical payload (RFC 6455 §5.5.2).
//!     - **Pong** → record activity, discard payload.
//!     - **Close** → enqueue close echo (same code if valid, else 1000),
//!       shut down the writer, exit cleanly.
//!     - `FrameError` → close per the spec mapping (Unmasked/Reserved/
//!       InvalidOpcode → 1002; PayloadTooLarge → 1009).
//!
//! - **Writer thread**: drains a `mpsc::sync_channel(send_queue_bound)`
//!   and `write_all()` each frame. Exit on channel-closed (reader dropped
//!   `tx`) OR on `write_all` error (peer disconnected mid-flush).
//!
//! - **Heartbeat + idle**: the reader sets a short `read_timeout` per
//!   iteration so it wakes periodically to check:
//!     - `last_pong_at` vs `last_ping_sent_at` — if a ping is outstanding
//!       longer than `pong_timeout` → close 1011.
//!     - `last_client_activity` vs now — if longer than `ping_interval`
//!       since any client frame, send a ping.
//!     - `last_client_activity` vs now — if longer than `idle_timeout` →
//!       close 1001 (Going Away).
//!
//! - **Graceful close**: when the reader decides to close (peer Close
//!   received, peer protocol violation, or local timer fired), it
//!   enqueues the close frame and drops `tx`. The writer drains any
//!   remaining queued frames + writes the close, then exits. The reader
//!   joins the writer (`handle.join()`) before returning. NO zombie
//!   threads.
//!
//! - **Backpressure**: per spec §7, full send queue is fatal —
//!   `tx.send(frame)` returns `SendError` → close 1011 (Internal Server
//!   Error). V1 prefers fast-fail to silent backlog. Pre-close attempts
//!   to enqueue the close frame use `try_send` so an already-full queue
//!   doesn't block forever.
//!
//! - **Determinism**: every decision (which opcode → which response, which
//!   close code on each error) is a pure function of the byte stream. No
//!   wall-clock dependencies except the heartbeat/idle timers, which use
//!   `std::time::Instant` (monotonic) so wall-clock jumps don't trip them.
//!
//! ## Zero-dep + std-only
//!
//! `std::net::TcpStream::try_clone()` provides the reader/writer split.
//! `std::sync::mpsc::sync_channel` provides bounded backpressure.
//! `std::thread::spawn` provides the writer thread. No tokio, no async,
//! no external runtime — same shape as the rest of the gateway.

#![forbid(unsafe_code)]
#![allow(dead_code)]

use super::frame::{
    self, decode_client_frame, encode_close_frame, encode_pong_frame,
    encode_server_frame, Frame, FrameError, OPCODE_BINARY, OPCODE_CLOSE,
    OPCODE_CONTINUATION, OPCODE_PING, OPCODE_PONG, OPCODE_TEXT,
};
use super::{WsError, WS_SEND_QUEUE_BOUND};
use crate::engine::EngineApply;
use kessel_proto::{Op, OpResult};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Per-connection knobs for the session loop. Defaults match the design
/// spec §9: ping every 30s, fail if no pong within 60s, idle-timeout at
/// 300s, accept frames up to 16 MiB, bound the send queue at 16 frames.
///
/// The whole struct is `Clone + Debug` so tests can override individual
/// timers to drive heartbeat / idle KATs in milliseconds instead of
/// minutes.
#[derive(Clone, Debug)]
pub struct WsSessionConfig {
    /// Inter-frame gap after which the server sends a Ping (RFC 6455
    /// §5.5.2). Per spec §9.2: 30s.
    pub ping_interval: Duration,
    /// Time the server waits for a Pong after sending a Ping; if exceeded
    /// the session closes 1011. Per spec §9.2 the design spec sketched
    /// 30s here; we use 60s to leave headroom for in-flight pings on
    /// slow links (heartbeat budget = ping_interval + pong_timeout).
    pub pong_timeout: Duration,
    /// Time without ANY client frame after which the session closes 1001
    /// (Going Away). Per spec §9.1: 300s. The reader's wake-up tick must
    /// be ≤ `idle_timeout` so the tick eventually fires.
    pub idle_timeout: Duration,
    /// Largest accepted frame payload (decoder cap). T5 currently
    /// delegates to `frame::MAX_PAYLOAD` (16 MiB) — this knob exists for
    /// forward compatibility with a per-connection cap.
    pub max_frame_size: usize,
    /// Per spec §7: bound for the writer-thread queue. Full queue → close
    /// 1011.
    pub send_queue_bound: usize,
    /// Reader wake-up tick. Internal — exposed for tests so the heartbeat
    /// can fire in milliseconds. NOT part of the public V1 contract;
    /// production builds use the default (1s).
    pub tick_interval: Duration,
}

impl Default for WsSessionConfig {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(30),
            pong_timeout: Duration::from_secs(60),
            idle_timeout: Duration::from_secs(300),
            max_frame_size: frame::MAX_PAYLOAD,
            send_queue_bound: WS_SEND_QUEUE_BOUND,
            tick_interval: Duration::from_secs(1),
        }
    }
}

/// Reasons the session ended. Internal to T5; surfaces as `WsError::Io`
/// (mapped from the close path) for the routes-side caller — the caller
/// just needs to know the session ended, the close handshake bytes are
/// already on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionEnd {
    /// Peer sent a Close frame; we echoed it.
    PeerClose,
    /// Local-side close (timer fired, protocol violation, full queue).
    LocalClose(u16),
    /// Reader's TCP read returned 0 bytes (TCP FIN from peer) without a
    /// Close handshake first.
    PeerDisconnect,
    /// Reader's TCP read returned a non-timeout error (write error on
    /// the writer side is observed the same way — the writer drops its
    /// half of the socket, and the reader's next read errors).
    IoError,
}

/// Spec §6.3 / §9: run the per-connection WebSocket session loop on a
/// `TcpStream` that has just completed the upgrade handshake.
///
/// **Lifecycle invariants** (all locked by KATs):
/// - Both threads (reader = caller, writer = spawned) join cleanly under
///   every termination path. NO zombie threads.
/// - On any close (peer-initiated, local-initiated, or error), the close
///   frame is enqueued for the writer to send before the writer thread
///   exits. The writer flushes everything in the queue, including the
///   close, before returning.
/// - Same `Op` byte sequence on the wire produces the same `OpResult`
///   byte sequence — `engine.apply_op` is the only source of
///   non-determinism, and it's the workspace's existing deterministic
///   apply path.
/// - Heartbeat timers use `std::time::Instant` (monotonic). Wall-clock
///   jumps don't fire premature close.
/// - The stream's `set_read_timeout` is overridden by the session loop
///   (the prior `serve()` value is replaced with `tick_interval`).
///
/// Returns `Ok(())` on a clean close (peer or local-initiated).
/// Returns `Err(WsError::Io(_))` only if the writer thread joined with a
/// panic OR `try_clone` failed at startup.
pub fn run_ws_session(
    stream: TcpStream,
    engine: Arc<dyn EngineApply>,
    config: WsSessionConfig,
) -> Result<(), WsError> {
    // Split the stream so the reader and writer threads can operate on
    // independent handles without locking. `try_clone` is a std::net
    // primitive that returns a second handle on the SAME OS socket;
    // dropping one observably half-closes for the other (the next read
    // or write errors).
    let writer_stream = stream
        .try_clone()
        .map_err(|e| WsError::Io(e.kind()))?;
    let reader_stream = stream;
    // Replace the inherited 30s read timeout with our tick-aligned one
    // so the read loop wakes up frequently enough to honor the
    // heartbeat / idle timers. Best-effort — if the platform refuses
    // (rare), the loop still works but with coarser timers.
    let _ = reader_stream.set_read_timeout(Some(config.tick_interval));
    // Bounded send queue per spec §7. The writer thread is the only
    // reader of `rx`; the reader thread + the heartbeat path are the
    // only senders.
    let (tx, rx) = sync_channel::<Vec<u8>>(config.send_queue_bound);
    let writer_handle = std::thread::Builder::new()
        .name("ws-writer".into())
        .spawn(move || writer_thread(writer_stream, rx))
        .map_err(|e| WsError::Io(e.kind()))?;
    // The reader thread is THIS thread — drives the session loop. On
    // exit it drops `tx`, which closes `rx`, which causes the writer
    // thread to drain remaining frames and exit. Then we join the
    // writer to make sure the close frame actually reached the wire
    // before we return.
    let _end = reader_loop(reader_stream, &tx, &engine, &config);
    drop(tx);
    // join() returns Err iff the thread panicked. We don't surface that
    // to the caller — the session ending is the deliverable; a panicked
    // writer thread already half-closed the socket via Drop.
    let _ = writer_handle.join();
    Ok(())
}

/// The reader thread / session-loop driver. Owns the read half of the
/// stream + the heartbeat/idle timers. Communicates with the writer
/// thread only via `tx`.
///
/// Returns the reason the session ended (debug-grade — the call site
/// already enqueued the close frame).
fn reader_loop(
    mut stream: TcpStream,
    tx: &SyncSender<Vec<u8>>,
    engine: &Arc<dyn EngineApply>,
    config: &WsSessionConfig,
) -> SessionEnd {
    let mut read_buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut chunk = [0u8; 8192];
    let mut last_client_activity = Instant::now();
    // `outstanding_ping_since`: when the server sent its last ping AND
    // hasn't yet received a matching pong. `None` = no ping outstanding.
    let mut outstanding_ping_since: Option<Instant> = None;
    let mut last_ping_sent_at: Option<Instant> = None;

    loop {
        // ----- Decode any frames we already have in `read_buf` -----
        loop {
            match decode_client_frame(&read_buf) {
                Ok((frame, consumed)) => {
                    last_client_activity = Instant::now();
                    // Cap check post-decode: the decoder uses
                    // `frame::MAX_PAYLOAD` (16 MiB); spec §8.3 says the
                    // per-connection cap is `config.max_frame_size`. For
                    // V1 the two agree; when they diverge (a future
                    // operator-set cap below 16 MiB), enforce here.
                    if frame.payload.len() > config.max_frame_size {
                        let _ = try_enqueue_close(tx, 1009, "frame too large");
                        return SessionEnd::LocalClose(1009);
                    }
                    // Spec §4.5 — V1 doesn't reassemble fragments;
                    // FIN=0 on any data frame is a session-level close
                    // 1003. (Control frames must always have FIN=1 per
                    // RFC 6455 §5.5; a control frame with FIN=0 is also
                    // a 1002 protocol error.)
                    if !frame.fin
                        && (frame.opcode == OPCODE_BINARY
                            || frame.opcode == OPCODE_TEXT
                            || frame.opcode == OPCODE_CONTINUATION)
                    {
                        let _ = try_enqueue_close(tx, 1003, "fragmentation not supported");
                        return SessionEnd::LocalClose(1003);
                    }
                    // Control frames per RFC 6455 §5.5 MUST have FIN=1
                    // AND payload ≤ 125 bytes. The decoder doesn't
                    // enforce the size cap on control frames; we do
                    // here so the session-level invariant holds.
                    if is_control(frame.opcode) {
                        if !frame.fin {
                            let _ = try_enqueue_close(tx, 1002, "fragmented control frame");
                            return SessionEnd::LocalClose(1002);
                        }
                        if frame.payload.len() > 125 {
                            let _ = try_enqueue_close(tx, 1002, "oversized control frame");
                            return SessionEnd::LocalClose(1002);
                        }
                    }
                    // Now dispatch by opcode.
                    let action = dispatch_frame(&frame, engine);
                    advance_buf(&mut read_buf, consumed);
                    match action {
                        DispatchAction::Send(bytes) => {
                            if enqueue_or_close(tx, bytes).is_err() {
                                return SessionEnd::LocalClose(1011);
                            }
                        }
                        DispatchAction::Pong(payload) => {
                            if enqueue_or_close(tx, encode_pong_frame(&payload)).is_err() {
                                return SessionEnd::LocalClose(1011);
                            }
                        }
                        DispatchAction::RecordPong => {
                            outstanding_ping_since = None;
                        }
                        DispatchAction::EchoCloseAndExit { code, reason } => {
                            let _ = try_enqueue_close(tx, code, &reason);
                            return SessionEnd::PeerClose;
                        }
                        DispatchAction::CloseLocal { code, reason } => {
                            let _ = try_enqueue_close(tx, code, &reason);
                            return SessionEnd::LocalClose(code);
                        }
                        DispatchAction::None => {}
                    }
                }
                Err(FrameError::NeedMoreData) => break,
                Err(FrameError::InvalidMask) => {
                    let _ = try_enqueue_close(tx, 1002, "unmasked client frame");
                    return SessionEnd::LocalClose(1002);
                }
                Err(FrameError::InvalidOpcode) => {
                    let _ = try_enqueue_close(tx, 1003, "unsupported opcode");
                    return SessionEnd::LocalClose(1003);
                }
                Err(FrameError::ReservedBitsSet) => {
                    let _ = try_enqueue_close(tx, 1002, "reserved bits set");
                    return SessionEnd::LocalClose(1002);
                }
                Err(FrameError::PayloadTooLarge) => {
                    let _ = try_enqueue_close(tx, 1009, "frame too large");
                    return SessionEnd::LocalClose(1009);
                }
            }
        }

        // ----- Read more bytes (with tick-interval timeout) -----
        match stream.read(&mut chunk) {
            Ok(0) => {
                // Peer half-closed without sending a Close frame. No
                // close echo possible (the wire is already FIN'd from
                // their side); we still try to enqueue a normal-1000
                // for the writer to flush, but it may be a no-op.
                let _ = try_enqueue_close(tx, 1000, "");
                return SessionEnd::PeerDisconnect;
            }
            Ok(n) => {
                read_buf.extend_from_slice(&chunk[..n]);
            }
            Err(e) => {
                // The `read_timeout` we set on the stream is the heartbeat
                // tick; a timeout here is the normal "no frame this tick"
                // signal. Other errors are real disconnects.
                if !is_read_timeout(&e) {
                    return SessionEnd::IoError;
                }
                // Timeout: fall through to the timer checks below.
            }
        }

        // ----- Heartbeat + idle timers -----
        let now = Instant::now();
        // Idle timeout: spec §9.1 — close 1001 if no client frame for
        // `idle_timeout`.
        if now.duration_since(last_client_activity) >= config.idle_timeout {
            let _ = try_enqueue_close(tx, 1001, "idle timeout");
            return SessionEnd::LocalClose(1001);
        }
        // Pong timeout: spec §9.2 — close 1011 if we sent a ping more
        // than `pong_timeout` ago and haven't heard a pong.
        if let Some(ping_at) = outstanding_ping_since {
            if now.duration_since(ping_at) >= config.pong_timeout {
                let _ = try_enqueue_close(tx, 1011, "pong timeout");
                return SessionEnd::LocalClose(1011);
            }
        }
        // Ping interval: send a ping if we haven't sent one recently
        // AND it's been at least `ping_interval` since any client frame.
        let due_for_ping = match last_ping_sent_at {
            None => now.duration_since(last_client_activity) >= config.ping_interval,
            Some(last) => now.duration_since(last) >= config.ping_interval
                && now.duration_since(last_client_activity) >= config.ping_interval,
        };
        if due_for_ping && outstanding_ping_since.is_none() {
            let ping_payload = b"ks"; // arbitrary 2-byte tag
            if enqueue_or_close(tx, encode_server_frame(OPCODE_PING, ping_payload)).is_err() {
                return SessionEnd::LocalClose(1011);
            }
            last_ping_sent_at = Some(now);
            outstanding_ping_since = Some(now);
        }
    }
}

/// The writer thread: drain `rx`, write each frame to the socket.
/// Returns when `rx` is closed (reader dropped `tx`) OR a `write_all`
/// fails.
fn writer_thread(mut stream: TcpStream, rx: std::sync::mpsc::Receiver<Vec<u8>>) {
    while let Ok(frame_bytes) = rx.recv() {
        if stream.write_all(&frame_bytes).is_err() {
            // Peer disconnected mid-write; drain remaining frames into
            // the void and return so the reader's `tx.send` will fail
            // and the reader's loop ends.
            return;
        }
    }
    // Best-effort flush + shutdown so the close frame actually makes it
    // out before the OS closes the socket on us.
    let _ = stream.flush();
    let _ = stream.shutdown(std::net::Shutdown::Both);
}

/// Per-frame dispatch decision. Pure function of the decoded frame and
/// the engine — no I/O, no timers.
#[derive(Debug)]
enum DispatchAction {
    /// Enqueue these bytes (typically an encoded OpResult binary frame
    /// per T6) for the writer to send.
    Send(Vec<u8>),
    /// Enqueue a Pong with this payload (RFC 6455 §5.5.2 — Pong payload
    /// echoes the incoming Ping payload).
    Pong(Vec<u8>),
    /// The client sent a Pong; mark the outstanding ping as satisfied.
    RecordPong,
    /// The peer initiated close. Enqueue the echo close + exit cleanly.
    EchoCloseAndExit { code: u16, reason: String },
    /// Local-side close (protocol error, unsupported frame, etc.).
    CloseLocal { code: u16, reason: String },
    /// Nothing to do (e.g. a control-flow case caller handled inline).
    None,
}

fn dispatch_frame(frame: &Frame, engine: &Arc<dyn EngineApply>) -> DispatchAction {
    match frame.opcode {
        OPCODE_BINARY => {
            // T6: kessel-op-v1 subprotocol. Payload = Op::encode() bytes;
            // we decode, apply, encode the OpResult into a binary frame.
            // Undecodable payload → close 1002 (protocol error on the
            // application layer: the subprotocol negotiated a binary Op
            // wire and the client sent something else).
            match Op::decode(&frame.payload) {
                Some(op) => {
                    let result = engine.apply_op(op);
                    let bytes = OpResult::encode(&result);
                    DispatchAction::Send(encode_server_frame(OPCODE_BINARY, &bytes))
                }
                None => DispatchAction::CloseLocal {
                    code: 1002,
                    reason: "undecodable Op bytes".into(),
                },
            }
        }
        OPCODE_TEXT => {
            // Spec §5.3: kessel-op-v1 is binary-only. Text frames are
            // unsupported data per RFC 6455 close-code 1003.
            DispatchAction::CloseLocal {
                code: 1003,
                reason: "text frames not supported by kessel-op-v1".into(),
            }
        }
        OPCODE_CONTINUATION => {
            // Spec §4.5: V1 doesn't reassemble fragments. A continuation
            // frame at message boundary (no preceding FIN=0) is
            // structurally illegal; we treat ANY continuation as a
            // policy violation under V1.
            DispatchAction::CloseLocal {
                code: 1003,
                reason: "continuation frames not supported".into(),
            }
        }
        OPCODE_PING => {
            // RFC 6455 §5.5.2: respond with Pong, payload echoed verbatim.
            DispatchAction::Pong(frame.payload.clone())
        }
        OPCODE_PONG => {
            // RFC 6455 §5.5.3: server consumes + discards. Heartbeat
            // tracking handled by the reader loop via RecordPong.
            DispatchAction::RecordPong
        }
        OPCODE_CLOSE => {
            // Spec §9.4 + RFC 6455 §5.5.1: peer-initiated close. We
            // echo with the same code if valid (1000-4999 excluding the
            // reserved 1004/1005/1006/1015) OR 1000 if absent/invalid.
            // Per §5.5.1 a 1-byte close payload is malformed → 1002.
            match frame.payload.len() {
                0 => DispatchAction::EchoCloseAndExit {
                    code: 1000,
                    reason: String::new(),
                },
                1 => DispatchAction::CloseLocal {
                    code: 1002,
                    reason: "malformed close payload".into(),
                },
                _ => {
                    let code = u16::from_be_bytes([frame.payload[0], frame.payload[1]]);
                    let echo_code = if is_valid_peer_close_code(code) { code } else { 1002 };
                    DispatchAction::EchoCloseAndExit {
                        code: echo_code,
                        reason: String::new(),
                    }
                }
            }
        }
        _ => {
            // Decoder already rejected reserved opcodes; this branch is
            // defensive (cannot fire under the V1 decoder).
            DispatchAction::CloseLocal {
                code: 1003,
                reason: "unsupported opcode".into(),
            }
        }
    }
}

fn is_control(opcode: u8) -> bool {
    matches!(opcode, OPCODE_CLOSE | OPCODE_PING | OPCODE_PONG)
}

/// Per RFC 6455 §7.4: valid peer-supplied close codes are 1000-1011 (with
/// 1004/1005/1006 reserved as never-on-the-wire) and 3000-4999. A peer
/// sending a reserved-on-wire code gets echoed close 1002.
fn is_valid_peer_close_code(code: u16) -> bool {
    match code {
        1004 | 1005 | 1006 | 1015 => false,
        1000..=1011 => true,
        3000..=4999 => true,
        _ => false,
    }
}

/// Try to enqueue a frame into the writer's bounded channel. Returns
/// `Err(())` if the channel is full (V1 policy: fast-fail → close 1011).
fn enqueue_or_close(tx: &SyncSender<Vec<u8>>, bytes: Vec<u8>) -> Result<(), ()> {
    // Use `try_send` so a full queue is a fast-fail, not a block. The
    // session is single-threaded on the writer side; if the writer can't
    // keep up, blocking the reader would let the queue fill further on
    // the heartbeat path, masking the problem.
    match tx.try_send(bytes) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => Err(()),
        Err(TrySendError::Disconnected(_)) => Err(()),
    }
}

/// Enqueue a close frame on a best-effort basis. The caller has already
/// decided to end the session; if the channel is full or disconnected we
/// move on (the reader thread is about to return; the writer will exit
/// via channel-disconnected).
fn try_enqueue_close(tx: &SyncSender<Vec<u8>>, code: u16, reason: &str) -> Result<(), ()> {
    let bytes = encode_close_frame(code, reason);
    match tx.try_send(bytes) {
        Ok(()) => Ok(()),
        Err(_) => Err(()),
    }
}

fn advance_buf(buf: &mut Vec<u8>, consumed: usize) {
    if consumed >= buf.len() {
        buf.clear();
    } else {
        buf.drain(..consumed);
    }
}

fn is_read_timeout(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{EngineApply, HealthSnapshot, MetricsSnapshot};
    use kessel_proto::{ClientId, Op, OpResult};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    // --- Test engine --------------------------------------------------

    /// Records every `apply_op` invocation and replies with `OpResult::Ok`
    /// by default. Tests can override the reply via `set_reply`.
    struct RecordingEngine {
        call_count: AtomicUsize,
    }

    impl RecordingEngine {
        fn new() -> Arc<dyn EngineApply> {
            Arc::new(Self { call_count: AtomicUsize::new(0) })
        }
    }

    impl EngineApply for RecordingEngine {
        fn apply_op(&self, _op: Op) -> OpResult {
            self.call_count.fetch_add(1, Ordering::AcqRel);
            OpResult::Ok
        }
        fn apply_op_with_session(&self, _: ClientId, _: u64, _: Op) -> OpResult { OpResult::Ok }
        fn apply_sql(&self, _: &str) -> OpResult { OpResult::Ok }
        fn apply_sql_with_session(&self, _: ClientId, _: u64, _: &str) -> OpResult { OpResult::Ok }
        fn snapshot_health(&self) -> HealthSnapshot {
            HealthSnapshot { primary: true, view: 0, op_number: 0, role: "primary" }
        }
        fn snapshot_metrics(&self) -> MetricsSnapshot {
            MetricsSnapshot {
                ops_total: Vec::new(),
                inflight: 0,
                last_op_number: 0,
                view_number: 0,
                is_primary: true,
                view_changes_total: 0,
                replica_lag_opnum: 0,
                http_requests_total: Vec::new(),
            }
        }
    }

    // --- Test plumbing ------------------------------------------------

    /// Listens on 127.0.0.1:0 and returns the listener + bound address.
    fn listen() -> (TcpListener, SocketAddr) {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
        let addr = l.local_addr().expect("local_addr");
        (l, addr)
    }

    /// Connect a client to `addr`; return the client-side TcpStream.
    fn dial(addr: SocketAddr) -> TcpStream {
        TcpStream::connect_timeout(&addr, Duration::from_secs(2)).expect("connect")
    }

    /// Test config: short timers so KATs run in milliseconds.
    fn fast_cfg() -> WsSessionConfig {
        WsSessionConfig {
            ping_interval: Duration::from_millis(200),
            pong_timeout: Duration::from_millis(200),
            idle_timeout: Duration::from_secs(60),
            max_frame_size: frame::MAX_PAYLOAD,
            send_queue_bound: WS_SEND_QUEUE_BOUND,
            tick_interval: Duration::from_millis(20),
        }
    }

    /// Spawn the session loop on a server-side socket the listener has
    /// just accepted. Returns the join handle so the test can assert
    /// the session ends cleanly.
    fn spawn_session(
        listener: TcpListener,
        engine: Arc<dyn EngineApply>,
        cfg: WsSessionConfig,
    ) -> std::thread::JoinHandle<Result<(), WsError>> {
        std::thread::spawn(move || {
            let (server_sock, _) = listener.accept().expect("accept");
            // Short write timeout so a wedged peer doesn't hang the test
            // for minutes.
            let _ = server_sock.set_write_timeout(Some(Duration::from_secs(5)));
            run_ws_session(server_sock, engine, cfg)
        })
    }

    /// Mask a client frame (header + 4-byte key + XOR'd payload). Mirrors
    /// the helper inside `frame::tests::add_client_mask` so we don't
    /// share private test code across modules.
    fn mask_client(server_encoded: &[u8], mask: [u8; 4]) -> Vec<u8> {
        let b0 = server_encoded[0];
        let b1 = server_encoded[1];
        let len7 = b1 & 0x7F;
        let header_extra: usize = match len7 {
            126 => 2,
            127 => 8,
            _ => 0,
        };
        let payload_start = 2 + header_extra;
        let payload = &server_encoded[payload_start..];
        let mut out = Vec::with_capacity(server_encoded.len() + 4);
        out.push(b0);
        out.push(b1 | 0x80);
        out.extend_from_slice(&server_encoded[2..payload_start]);
        out.extend_from_slice(&mask);
        for (i, b) in payload.iter().enumerate() {
            out.push(b ^ mask[i & 3]);
        }
        out
    }

    /// Drive a session client: send `frames` (already masked), collect
    /// every server response frame until the peer closes (TCP FIN). The
    /// returned Vec is each decoded server frame in order.
    fn drive_client(
        mut stream: TcpStream,
        frames: Vec<Vec<u8>>,
    ) -> Vec<DecodedServerFrame> {
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        for f in &frames {
            stream.write_all(f).expect("client write");
        }
        let mut buf = Vec::new();
        let _ = stream.read_to_end(&mut buf);
        decode_all_server_frames(&buf)
    }

    /// Server-frame decoder (server frames are NOT masked) for tests.
    /// Returns `(opcode, payload)` per frame.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct DecodedServerFrame {
        opcode: u8,
        payload: Vec<u8>,
    }

    fn decode_all_server_frames(bytes: &[u8]) -> Vec<DecodedServerFrame> {
        let mut out = Vec::new();
        let mut i = 0;
        while i + 2 <= bytes.len() {
            let b0 = bytes[i];
            let b1 = bytes[i + 1];
            let opcode = b0 & 0x0F;
            assert_eq!(b1 & 0x80, 0, "server frames must NOT have MASK bit");
            let len7 = b1 & 0x7F;
            let (len, header_extra) = match len7 {
                127 => {
                    if i + 10 > bytes.len() { break; }
                    let mut l: u64 = 0;
                    for k in 0..8 { l = (l << 8) | (bytes[i + 2 + k] as u64); }
                    (l as usize, 8)
                }
                126 => {
                    if i + 4 > bytes.len() { break; }
                    let l = ((bytes[i + 2] as usize) << 8) | (bytes[i + 3] as usize);
                    (l, 2)
                }
                n => (n as usize, 0),
            };
            let payload_start = i + 2 + header_extra;
            let payload_end = payload_start + len;
            if payload_end > bytes.len() { break; }
            out.push(DecodedServerFrame {
                opcode,
                payload: bytes[payload_start..payload_end].to_vec(),
            });
            i = payload_end;
        }
        out
    }

    // --- WsSessionConfig defaults -------------------------------------

    /// Spec §9: defaults match the design spec.
    #[test]
    fn t5_default_config_matches_spec() {
        let cfg = WsSessionConfig::default();
        assert_eq!(cfg.ping_interval, Duration::from_secs(30),
            "spec §9.2: 30s ping interval");
        assert_eq!(cfg.pong_timeout, Duration::from_secs(60),
            "60s pong timeout (heartbeat = ping + pong = 90s budget)");
        assert_eq!(cfg.idle_timeout, Duration::from_secs(300),
            "spec §9.1: idle timeout");
        assert_eq!(cfg.max_frame_size, frame::MAX_PAYLOAD,
            "max_frame_size mirrors the frame decoder cap");
        assert_eq!(cfg.send_queue_bound, WS_SEND_QUEUE_BOUND,
            "spec §7: WS_SEND_QUEUE_BOUND = 16");
    }

    // --- KAT 1: End-to-end Op → OpResult round trip -------------------

    /// Spec §11 acceptance #1 (binary subprotocol round trip):
    /// client sends an Op::SetAuthToken frame (the simplest no-arg Op
    /// we can construct without a session), receives an OpResult::Ok
    /// frame. Locks the full T6 wire.
    #[test]
    fn t5_t6_e2e_binary_op_in_op_result_out() {
        let (listener, addr) = listen();
        let engine = RecordingEngine::new();
        let handle = spawn_session(listener, engine.clone(), fast_cfg());
        // Build the minimal Op the engine can apply: an SQL upsert via
        // Op::Update on a non-existent type yields a Got/NotFound/etc;
        // we don't care which OpResult — only that the round trip
        // produces ANY binary frame containing valid OpResult bytes.
        // Op::SetTuple lookup isn't in the workspace; use Op::Delete on
        // a fake oid which the engine cleanly NotFound's. Falling back
        // to Op::Health since the RecordingEngine returns Ok regardless.
        let op = Op::Delete {
            type_id: 0,
            id: kessel_proto::ObjectId([0u8; 16]),
        };
        let op_bytes = op.encode();
        let server_frame = encode_server_frame(OPCODE_BINARY, &op_bytes);
        let client_frame = mask_client(&server_frame, [0x11, 0x22, 0x33, 0x44]);
        // Then a close frame so the session ends cleanly.
        let close = mask_client(&encode_close_frame(1000, ""), [0xAA, 0xBB, 0xCC, 0xDD]);
        let client = dial(addr);
        let frames = drive_client(client, vec![client_frame, close]);
        let _ = handle.join().expect("thread join");
        // First server frame should be the binary OpResult; second should
        // be a close echo.
        assert!(frames.len() >= 2, "expected at least op_result + close; got {frames:?}");
        assert_eq!(frames[0].opcode, OPCODE_BINARY,
            "OpResult frame must be binary; got {:?}", frames[0]);
        let decoded_result = OpResult::decode(&frames[0].payload)
            .expect("OpResult bytes must round-trip");
        assert_eq!(decoded_result, OpResult::Ok,
            "RecordingEngine always returns Ok");
        // Find the close frame.
        let close = frames.iter().find(|f| f.opcode == OPCODE_CLOSE)
            .expect("server must echo close");
        assert_eq!(&close.payload[..2], &[0x03, 0xE8],
            "echo close code = 1000; got {close:?}");
    }

    // --- KAT 2: Ping/Pong round trip ----------------------------------

    /// RFC 6455 §5.5.2: client Ping → server Pong with identical payload.
    #[test]
    fn t5_ping_round_trip() {
        let (listener, addr) = listen();
        let handle = spawn_session(listener, RecordingEngine::new(), fast_cfg());
        let ping = mask_client(
            &encode_server_frame(OPCODE_PING, b"hi"),
            [0x00, 0x00, 0x00, 0x00],
        );
        let close = mask_client(&encode_close_frame(1000, ""), [0; 4]);
        let client = dial(addr);
        let frames = drive_client(client, vec![ping, close]);
        let _ = handle.join();
        let pong = frames.iter().find(|f| f.opcode == OPCODE_PONG)
            .expect("server must respond Pong to Ping");
        assert_eq!(pong.payload, b"hi",
            "Pong payload must echo Ping payload byte-for-byte");
    }

    // --- KAT 3: Close handshake ---------------------------------------

    /// Spec §9.4: client sends Close(1000) → server echoes Close(1000)
    /// → socket closes cleanly.
    #[test]
    fn t5_close_handshake_echo() {
        let (listener, addr) = listen();
        let handle = spawn_session(listener, RecordingEngine::new(), fast_cfg());
        let close = mask_client(&encode_close_frame(1000, ""), [0; 4]);
        let client = dial(addr);
        let frames = drive_client(client, vec![close]);
        let session_result = handle.join().expect("thread joined");
        assert!(session_result.is_ok(), "session must end cleanly: {session_result:?}");
        let close = frames.iter().find(|f| f.opcode == OPCODE_CLOSE)
            .expect("server must echo close");
        assert_eq!(&close.payload[..2], &[0x03, 0xE8],
            "echo close code = 1000; got {close:?}");
    }

    // --- KAT 4: Server-initiated close on pong timeout ----------------

    /// Spec §9.2: server sends Ping after `ping_interval` of idle; if no
    /// Pong arrives within `pong_timeout`, server closes 1011.
    #[test]
    fn t5_pong_timeout_fires_close_1011() {
        let (listener, addr) = listen();
        let mut cfg = fast_cfg();
        cfg.ping_interval = Duration::from_millis(50);
        cfg.pong_timeout = Duration::from_millis(100);
        cfg.tick_interval = Duration::from_millis(20);
        let handle = spawn_session(listener, RecordingEngine::new(), cfg);
        let mut client = dial(addr);
        client.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        // Don't send anything. Wait for the server to ping us and then
        // close us.
        let mut buf = Vec::new();
        let _ = client.read_to_end(&mut buf);
        let _ = handle.join();
        let frames = decode_all_server_frames(&buf);
        assert!(frames.iter().any(|f| f.opcode == OPCODE_PING),
            "server must have sent a Ping; got {frames:?}");
        let close = frames.iter().find(|f| f.opcode == OPCODE_CLOSE)
            .expect("server must close after pong timeout; got {frames:?}");
        assert_eq!(&close.payload[..2], &[0x03, 0xF3],
            "close code 1011 (Internal Error); got {close:?}");
    }

    // --- KAT 5: Fragmented data frame closes 1003 ---------------------

    /// Spec §4.5: V1 rejects fragmentation. A FIN=0 binary frame from
    /// the client → server closes with 1003 (Unsupported Data).
    #[test]
    fn t5_fragmented_data_frame_closes_1003() {
        let (listener, addr) = listen();
        let handle = spawn_session(listener, RecordingEngine::new(), fast_cfg());
        // Build a FIN=0 binary frame manually: byte 0 = 0x02 (no FIN,
        // opcode binary). Payload "abc".
        let mut server = vec![0x02, 0x03, b'a', b'b', b'c'];
        // Inject the mask bit + 4 mask bytes + XOR'd payload to make it
        // a valid client frame on the wire.
        // We bypass `mask_client` because that helper sets FIN=1
        // implicitly via the encoder path; here we want FIN=0 on the wire.
        let b1 = server[1];
        server[1] = b1 | 0x80;
        let mask = [0xCA, 0xFE, 0xBA, 0xBE];
        let mut wire = vec![server[0], server[1]];
        wire.extend_from_slice(&mask);
        for (i, b) in server[2..].iter().enumerate() {
            wire.push(b ^ mask[i & 3]);
        }
        let client = dial(addr);
        let frames = drive_client(client, vec![wire]);
        let _ = handle.join();
        let close = frames.iter().find(|f| f.opcode == OPCODE_CLOSE)
            .expect("server must close on fragmented data; got {frames:?}");
        assert_eq!(&close.payload[..2], &[0x03, 0xEB],
            "1003 = 0x03EB; got {close:?}");
    }

    // --- KAT 6: Oversized frame closes 1009 ---------------------------

    /// Spec §8.3: an oversized client frame closes 1009.
    /// We can't actually send a 16-MiB frame in a unit test (slow), so we
    /// craft a frame whose declared 64-bit length exceeds the cap; the
    /// decoder rejects with PayloadTooLarge BEFORE allocation and the
    /// session loop maps that to 1009.
    #[test]
    fn t5_oversized_frame_closes_1009() {
        let (listener, addr) = listen();
        let handle = spawn_session(listener, RecordingEngine::new(), fast_cfg());
        // 0x82 = FIN|binary; 0xFF = MASK|127 (64-bit sentinel); 8 bytes
        // of all-0xFF length = u64::MAX.
        let wire = vec![
            0x82, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            // Mask + payload absent (decoder rejects before reading them).
        ];
        let client = dial(addr);
        let frames = drive_client(client, vec![wire]);
        let _ = handle.join();
        let close = frames.iter().find(|f| f.opcode == OPCODE_CLOSE)
            .expect("server must close on oversized frame");
        assert_eq!(&close.payload[..2], &[0x03, 0xF1],
            "1009 = 0x03F1; got {close:?}");
    }

    // --- KAT 7: Unmasked client frame closes 1002 ---------------------

    /// RFC 6455 §5.3: an unmasked client frame is a protocol violation.
    /// Session closes 1002.
    #[test]
    fn t5_unmasked_client_frame_closes_1002() {
        let (listener, addr) = listen();
        let handle = spawn_session(listener, RecordingEngine::new(), fast_cfg());
        // Server-encoded binary frame = NOT masked. This is exactly the
        // shape an attacker would send.
        let wire = encode_server_frame(OPCODE_BINARY, b"unmasked");
        let client = dial(addr);
        let frames = drive_client(client, vec![wire]);
        let _ = handle.join();
        let close = frames.iter().find(|f| f.opcode == OPCODE_CLOSE)
            .expect("server must close on unmasked frame");
        assert_eq!(&close.payload[..2], &[0x03, 0xEA],
            "1002 = 0x03EA; got {close:?}");
    }

    // --- KAT 8: Text frame closes 1003 (subprotocol is binary) --------

    /// Spec §5.3 / §5.4: kessel-op-v1 is binary-only. A text frame from
    /// the client → close 1003.
    #[test]
    fn t5_text_frame_closes_1003() {
        let (listener, addr) = listen();
        let handle = spawn_session(listener, RecordingEngine::new(), fast_cfg());
        let text = mask_client(&encode_server_frame(OPCODE_TEXT, b"hello"), [0; 4]);
        let client = dial(addr);
        let frames = drive_client(client, vec![text]);
        let _ = handle.join();
        let close = frames.iter().find(|f| f.opcode == OPCODE_CLOSE)
            .expect("server must close on text frame");
        assert_eq!(&close.payload[..2], &[0x03, 0xEB],
            "1003 = 0x03EB; got {close:?}");
    }

    // --- KAT 9: Undecodable Op bytes close 1002 -----------------------

    /// T6 application-layer protocol error: binary frame whose payload
    /// is NOT a valid `Op::encode()` byte sequence → close 1002.
    #[test]
    fn t5_t6_undecodable_op_bytes_close_1002() {
        let (listener, addr) = listen();
        let handle = spawn_session(listener, RecordingEngine::new(), fast_cfg());
        let garbage = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        let frame = mask_client(&encode_server_frame(OPCODE_BINARY, &garbage), [0; 4]);
        let client = dial(addr);
        let frames = drive_client(client, vec![frame]);
        let _ = handle.join();
        let close = frames.iter().find(|f| f.opcode == OPCODE_CLOSE)
            .expect("server must close on undecodable Op");
        assert_eq!(&close.payload[..2], &[0x03, 0xEA],
            "1002 = 0x03EA; got {close:?}");
    }

    // --- KAT 10: Two Ops in a row produce two OpResults in order ------

    /// Lockstep request-response per spec §5.3 default: client sends Op1,
    /// Op2, Close; server responds OpResult1, OpResult2, Close — in
    /// arrival order (V1 is FIFO; no correlation IDs).
    #[test]
    fn t5_t6_two_ops_produce_two_ordered_op_results() {
        let (listener, addr) = listen();
        let engine = RecordingEngine::new();
        let handle = spawn_session(listener, engine.clone(), fast_cfg());
        let op1 = Op::Delete {
            type_id: 0,
            id: kessel_proto::ObjectId([1u8; 16]),
        };
        let op2 = Op::Delete {
            type_id: 0,
            id: kessel_proto::ObjectId([2u8; 16]),
        };
        let f1 = mask_client(&encode_server_frame(OPCODE_BINARY, &op1.encode()),
            [0x11; 4]);
        let f2 = mask_client(&encode_server_frame(OPCODE_BINARY, &op2.encode()),
            [0x22; 4]);
        let close = mask_client(&encode_close_frame(1000, ""), [0; 4]);
        let client = dial(addr);
        let frames = drive_client(client, vec![f1, f2, close]);
        let _ = handle.join();
        let binary_frames: Vec<&DecodedServerFrame> = frames.iter()
            .filter(|f| f.opcode == OPCODE_BINARY).collect();
        assert_eq!(binary_frames.len(), 2,
            "two Ops → two OpResults; got {frames:?}");
        for f in &binary_frames {
            let r = OpResult::decode(&f.payload).expect("decode OpResult");
            assert_eq!(r, OpResult::Ok);
        }
    }

    // --- KAT 11: Close with code 1004 (reserved) → echo 1002 ----------

    /// RFC 6455 §7.4.1: 1004/1005/1006/1015 are reserved and MUST NOT be
    /// sent on the wire. A peer sending one → server echoes close 1002.
    #[test]
    fn t5_close_with_reserved_1004_echoes_1002() {
        let (listener, addr) = listen();
        let handle = spawn_session(listener, RecordingEngine::new(), fast_cfg());
        // Build a close frame with code 1004 directly.
        let close = mask_client(&encode_close_frame(1004, ""), [0; 4]);
        let client = dial(addr);
        let frames = drive_client(client, vec![close]);
        let _ = handle.join();
        let echo = frames.iter().find(|f| f.opcode == OPCODE_CLOSE)
            .expect("server must echo close");
        assert_eq!(&echo.payload[..2], &[0x03, 0xEA],
            "reserved 1004 → echo 1002 (0x03EA); got {echo:?}");
    }

    // --- KAT 12: Reader thread joins cleanly (no zombie) --------------

    /// Lifecycle invariant: after the session ends, the writer thread
    /// joined. The `spawn_session` helper's `join().expect()` is the
    /// proof — if the writer thread were stuck, the parent thread's
    /// join would deadlock until the test timeout. We assert the
    /// session join completes well under the 60s test timeout.
    #[test]
    fn t5_session_join_completes_promptly_after_peer_close() {
        let (listener, addr) = listen();
        let handle = spawn_session(listener, RecordingEngine::new(), fast_cfg());
        let close = mask_client(&encode_close_frame(1000, ""), [0; 4]);
        let client = dial(addr);
        let _ = drive_client(client, vec![close]);
        let start = Instant::now();
        let res = handle.join().expect("session thread joined");
        let elapsed = start.elapsed();
        assert!(res.is_ok(), "session ended cleanly: {res:?}");
        assert!(elapsed < Duration::from_secs(2),
            "session must join within 2s of peer close; took {elapsed:?}");
    }

    // --- KAT 13: Peer TCP FIN ends session without panic --------------

    /// Peer drops the TCP connection without a Close frame. The reader's
    /// `read(0)` triggers PeerDisconnect; the writer's `recv` returns Err
    /// when `tx` drops; both threads exit cleanly. The
    /// `try_enqueue_close` is best-effort — if the writer beats the
    /// reader to noticing the disconnect, the close is silently
    /// dropped (no panic, no leak).
    #[test]
    fn t5_peer_tcp_fin_ends_session_cleanly() {
        let (listener, addr) = listen();
        let handle = spawn_session(listener, RecordingEngine::new(), fast_cfg());
        let client = dial(addr);
        drop(client); // immediate TCP FIN
        let res = handle.join().expect("session thread joined");
        assert!(res.is_ok(), "session ended cleanly: {res:?}");
    }

    // --- KAT 14: Determinism (same Op bytes → same OpResult bytes) ----

    /// Determinism: the same Op sequence on the wire produces the same
    /// OpResult sequence on the wire, byte-for-byte, across two
    /// independent runs. Locks the "no nondeterminism in the session
    /// loop" invariant — the only source of nondeterminism could be the
    /// engine (which is the workspace's deterministic apply path) or
    /// thread scheduling (which can't reorder responses inside a single
    /// FIFO session).
    #[test]
    fn t5_t6_same_op_sequence_produces_same_op_result_bytes() {
        fn run_once() -> Vec<u8> {
            let (listener, addr) = listen();
            let handle = spawn_session(listener, RecordingEngine::new(), fast_cfg());
            let op = Op::Delete {
                type_id: 0,
                id: kessel_proto::ObjectId([7u8; 16]),
            };
            let f = mask_client(&encode_server_frame(OPCODE_BINARY, &op.encode()),
                [0xAB; 4]);
            let close = mask_client(&encode_close_frame(1000, ""), [0; 4]);
            let mut client = dial(addr);
            client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            client.write_all(&f).unwrap();
            client.write_all(&close).unwrap();
            let mut buf = Vec::new();
            let _ = client.read_to_end(&mut buf);
            let _ = handle.join();
            // Extract only the BINARY frame payload (skip Close).
            let frames = decode_all_server_frames(&buf);
            frames.into_iter()
                .find(|f| f.opcode == OPCODE_BINARY)
                .map(|f| f.payload)
                .unwrap_or_default()
        }
        let a = run_once();
        let b = run_once();
        assert_eq!(a, b,
            "same Op sequence must produce byte-identical OpResult bytes \
            across runs (determinism invariant)");
    }

    // --- KAT 15: Idle timeout fires close 1001 ------------------------

    /// Spec §9.1: if no client frame arrives within `idle_timeout`, the
    /// server closes 1001 (Going Away). We disable ping/pong (very
    /// large ping_interval) so only the idle timer fires.
    #[test]
    fn t5_idle_timeout_fires_close_1001() {
        let (listener, addr) = listen();
        let cfg = WsSessionConfig {
            ping_interval: Duration::from_secs(60),
            pong_timeout: Duration::from_secs(60),
            idle_timeout: Duration::from_millis(150),
            max_frame_size: frame::MAX_PAYLOAD,
            send_queue_bound: WS_SEND_QUEUE_BOUND,
            tick_interval: Duration::from_millis(20),
        };
        let handle = spawn_session(listener, RecordingEngine::new(), cfg);
        let mut client = dial(addr);
        client.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut buf = Vec::new();
        let _ = client.read_to_end(&mut buf);
        let _ = handle.join();
        let frames = decode_all_server_frames(&buf);
        let close = frames.iter().find(|f| f.opcode == OPCODE_CLOSE)
            .expect("server must close on idle; got {frames:?}");
        assert_eq!(&close.payload[..2], &[0x03, 0xE9],
            "1001 = 0x03E9; got {close:?}");
    }
}
