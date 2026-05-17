//! kessel-client: a minimal blocking TCP client.
//!
//! Wire framing (shared with kesseldb-server): each message is
//! `[u32 little-endian length][payload]`. Request payload = `Op::encode()`,
//! response payload = `OpResult::encode()`.

#![forbid(unsafe_code)]

use kessel_proto::wire::{read_frame, write_frame};
use kessel_proto::{Op, OpResult};
use std::io;
use std::net::{TcpStream, ToSocketAddrs};

pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        Ok(Client {
            stream: TcpStream::connect(addr)?,
        })
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
