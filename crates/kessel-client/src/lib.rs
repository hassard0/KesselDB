//! kessel-client: a minimal blocking TCP client.
//!
//! Wire framing (shared with kesseldb-server): each message is
//! `[u32 little-endian length][payload]`. Request payload = `Op::encode()`,
//! response payload = `OpResult::encode()`.

#![forbid(unsafe_code)]

use kessel_proto::wire::{read_frame, write_frame};
use kessel_proto::{ClientId, Op, OpResult};
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{SystemTime, UNIX_EPOCH};

/// A session-request frame: `[0xFD][client:u128 LE][req:u64 LE][Op::encode()]`.
/// Carries a *stable* `(client, req)` so the server dedupes a cross-node
/// retry (exactly-once on failover). `0xFE` = SQL, `0xFD` = session op,
/// otherwise a bare `Op::encode()`.
pub const SESSION_TAG: u8 = 0xFD;

/// Build a session-request frame. Public so tests/tools can replay an
/// exact `(client, req)` and verify exactly-once.
pub fn session_frame(client: ClientId, req: u64, op: &Op) -> Vec<u8> {
    let mut f = Vec::with_capacity(25 + 16);
    f.push(SESSION_TAG);
    f.extend_from_slice(&client.to_le_bytes());
    f.extend_from_slice(&req.to_le_bytes());
    f.extend_from_slice(&op.encode());
    f
}

/// Parse a `0xFD` session frame into `(client, req, op)`; `None` if it is
/// not a session frame or is malformed. Used by the server front.
pub fn parse_session_frame(f: &[u8]) -> Option<(ClientId, u64, Op)> {
    if f.first() != Some(&SESSION_TAG) || f.len() < 25 {
        return None;
    }
    let client = u128::from_le_bytes(f[1..17].try_into().ok()?);
    let req = u64::from_le_bytes(f[17..25].try_into().ok()?);
    let op = Op::decode(&f[25..])?;
    Some((client, req, op))
}

pub struct Client {
    stream: TcpStream,
}

/// Auth handshake tag (mirrors `kesseldb_server::AUTH_TAG`).
pub const AUTH_TAG: u8 = 0xFC;

/// Send `[0xFC] ++ token` and require an `Ok` reply. Used by both client
/// kinds when a server token is configured.
fn do_auth(stream: &mut TcpStream, token: &[u8]) -> io::Result<()> {
    let mut f = Vec::with_capacity(token.len() + 1);
    f.push(AUTH_TAG);
    f.extend_from_slice(token);
    write_frame(stream, &f)?;
    let resp = read_frame(stream)?;
    match OpResult::decode(&resp) {
        Some(OpResult::Ok) => Ok(()),
        _ => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "kesseldb: unauthorized (bad token)",
        )),
    }
}

impl Client {
    pub fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        Ok(Client {
            stream: TcpStream::connect(addr)?,
        })
    }

    /// Connect and authenticate with a shared-secret token (the server's
    /// `ServerConfig.token`). Fails with `PermissionDenied` if rejected.
    pub fn connect_authed(addr: impl ToSocketAddrs, token: &[u8]) -> io::Result<Self> {
        let mut stream = TcpStream::connect(addr)?;
        do_auth(&mut stream, token)?;
        Ok(Client { stream })
    }

    /// Send one op, block for its result.
    pub fn call(&mut self, op: &Op) -> io::Result<OpResult> {
        write_frame(&mut self.stream, &op.encode())?;
        let resp = read_frame(&mut self.stream)?;
        OpResult::decode(&resp)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad OpResult frame"))
    }

    /// Send a SQL statement (compiled server-side against the live catalog).
    /// Wire form: `[0xFE] ++ utf8`.
    pub fn sql(&mut self, sql: &str) -> io::Result<OpResult> {
        let mut frame = Vec::with_capacity(sql.len() + 1);
        frame.push(0xFE);
        frame.extend_from_slice(sql.as_bytes());
        write_frame(&mut self.stream, &frame)?;
        let resp = read_frame(&mut self.stream)?;
        OpResult::decode(&resp)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad OpResult frame"))
    }
}

/// A failover-aware cluster client. Holds the node address list and a
/// **stable session** (`client` id + monotonic `req`). On `Unavailable`
/// (the contacted node is not the active primary) or a connection error,
/// it rotates to the next node and **retries the same `(client, req)`** —
/// safe because the server is exactly-once (SP40/41): a re-delivered
/// committed request returns its cached reply, never re-executing.
pub struct ClusterClient {
    addrs: Vec<String>,
    idx: usize,
    stream: Option<TcpStream>,
    client: ClientId,
    req: u64,
    token: Option<Vec<u8>>,
}

impl ClusterClient {
    /// `addrs` = every node's client address. Any order; the client finds
    /// the primary by rotation.
    pub fn new(addrs: Vec<String>) -> Self {
        // Zero-dep unique-enough client id: wall-clock nanos folded with a
        // per-process/thread salt. Collisions only cost a wrong dedup for
        // *that* pair; a fresh process effectively never collides.
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let salt = std::process::id() as u128;
        let tid = {
            // hash the ThreadId debug string (no external deps)
            let s = format!("{:?}", std::thread::current().id());
            s.bytes().fold(1469598103934665603u128, |h, b| {
                (h ^ b as u128).wrapping_mul(1099511628211)
            })
        };
        ClusterClient {
            addrs,
            idx: 0,
            stream: None,
            client: nanos ^ (salt << 80) ^ tid,
            req: 0,
            token: None,
        }
    }

    /// Authenticate every (re)connection with this shared-secret token.
    pub fn with_token(mut self, token: Vec<u8>) -> Self {
        self.token = Some(token);
        self
    }

    /// This client's stable session id (for tests / failover tooling).
    pub fn client_id(&self) -> ClientId {
        self.client
    }

    /// The last request number used (0 before the first `call`).
    pub fn last_req(&self) -> u64 {
        self.req
    }

    fn conn(&mut self) -> io::Result<&mut TcpStream> {
        if self.stream.is_none() {
            let a = &self.addrs[self.idx % self.addrs.len()];
            let mut s = TcpStream::connect(a)?;
            if let Some(tok) = &self.token {
                do_auth(&mut s, tok)?;
            }
            self.stream = Some(s);
        }
        Ok(self.stream.as_mut().unwrap())
    }

    /// Submit `op` exactly-once with automatic failover. Bounded attempts
    /// (≈ a few full rotations) before surfacing the last error.
    pub fn call(&mut self, op: &Op) -> io::Result<OpResult> {
        self.req += 1;
        let frame = session_frame(self.client, self.req, op);
        let max_attempts = (self.addrs.len() * 4).max(8);
        let mut last_err: Option<io::Error> = None;
        for _ in 0..max_attempts {
            let attempt = (|| -> io::Result<OpResult> {
                let s = self.conn()?;
                write_frame(s, &frame)?;
                let resp = read_frame(s)?;
                OpResult::decode(&resp).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "bad OpResult frame")
                })
            })();
            match attempt {
                Ok(OpResult::Unavailable) => {
                    // Not the primary — rotate and retry the same (client,req).
                    self.stream = None;
                    self.idx = self.idx.wrapping_add(1);
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                Ok(other) => return Ok(other),
                Err(e) => {
                    self.stream = None;
                    self.idx = self.idx.wrapping_add(1);
                    last_err = Some(e);
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::TimedOut, "no primary reachable")
        }))
    }
}
