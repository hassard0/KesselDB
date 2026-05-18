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

/// Render an `OpResult` as a concise, human/agent-readable line. Pure and
/// total — used by the `kessel` CLI and safe to rely on in scripts.
pub fn format_result(r: &OpResult) -> String {
    match r {
        OpResult::Ok => "OK".to_string(),
        OpResult::TypeCreated(t) => format!("OK  (table created, type_id={t})"),
        OpResult::Exists => "EXISTS  (row already present)".to_string(),
        OpResult::NotFound => "NOT FOUND".to_string(),
        OpResult::Constraint(m) => format!("CONSTRAINT  {m}"),
        OpResult::SchemaError(m) => format!("ERROR  {m}"),
        OpResult::Unavailable => {
            "UNAVAILABLE  (this node is not the active primary — connect with a \
             cluster address list / ClusterClient)"
                .to_string()
        }
        OpResult::Unauthorized => {
            "UNAUTHORIZED  (missing or wrong --token)".to_string()
        }
        OpResult::Got(b) if b.len() == 16 => {
            // The common scalar reply (aggregate result is a 16-byte i128).
            format!("= {}  ({} bytes)", i128::from_le_bytes(b[..16].try_into().unwrap()), b.len())
        }
        OpResult::Got(b) => {
            format!("GOT  {} bytes  (use `DESCRIBE <table>` to decode rows)", b.len())
        }
    }
}

fn format_value(v: &kessel_codec::Value) -> String {
    use kessel_codec::Value::*;
    match v {
        Null => "NULL".to_string(),
        Uint(u) => u.to_string(),
        Int(i) => i.to_string(),
        Blob(b) => {
            let t: &[u8] = {
                // trim fixed-width Char zero padding for display
                let end = b.iter().rposition(|&x| x != 0).map_or(0, |i| i + 1);
                &b[..end]
            };
            if t.iter().all(|&c| (0x20..=0x7e).contains(&c)) {
                String::from_utf8_lossy(t).into_owned()
            } else {
                let mut s = String::from("0x");
                for x in t {
                    s.push_str(&format!("{x:02x}"));
                }
                s
            }
        }
    }
}

/// Decode `SELECT *` row bytes (`[u32 len][record]*`) against a wire type
/// definition (`DESCRIBE` output) and render an aligned text table.
/// `None` if the schema or row stream is malformed — the caller then
/// falls back to [`format_result`]. Pure and total.
pub fn render_rows(typedef: &[u8], rows: &[u8]) -> Option<String> {
    let (name, fields) = kessel_catalog::decode_type_def(typedef)?;
    let ot = kessel_catalog::ObjectType::from_def(name, fields);
    let headers: Vec<String> = ot.fields.iter().map(|f| f.name.clone()).collect();
    if headers.is_empty() {
        return None;
    }
    // Two wire shapes: a filtered `SELECT *` returns `[u32 len][rec]*`;
    // the `SELECT * ... ID <n>` O(1) fast path returns a single bare
    // record. Try the length-prefixed form first (must consume exactly);
    // otherwise treat the whole blob as one record.
    let parse_lp = || -> Option<Vec<Vec<String>>> {
        let mut t = Vec::new();
        let mut p = 0usize;
        while p + 4 <= rows.len() {
            let len = u32::from_le_bytes(rows[p..p + 4].try_into().ok()?) as usize;
            p += 4;
            let rec = rows.get(p..p + len)?;
            p += len;
            let vals = kessel_codec::decode(&ot, rec).ok()?;
            t.push(vals.iter().map(format_value).collect());
        }
        (p == rows.len()).then_some(t)
    };
    let table: Vec<Vec<String>> = if let Some(t) = parse_lp() {
        t
    } else if !rows.is_empty() {
        // single bare record (primary-key fast path)
        let vals = kessel_codec::decode(&ot, rows).ok()?;
        vec![vals.iter().map(format_value).collect()]
    } else {
        Vec::new()
    };
    Some(render_table(&headers, &table))
}

/// Format a header + string-cell rows as an aligned ASCII table with an
/// `(N row[s])` footer. Shared by whole-row and projection rendering.
fn render_table(headers: &[String], table: &[Vec<String>]) -> String {
    let mut w: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in table {
        for (i, cell) in row.iter().enumerate() {
            if i < w.len() {
                w[i] = w[i].max(cell.len());
            }
        }
    }
    let pad = |s: &str, n: usize| format!("{s:<n$}");
    let mut out = String::new();
    out.push_str(
        &headers
            .iter()
            .enumerate()
            .map(|(i, h)| pad(h, w[i]))
            .collect::<Vec<_>>()
            .join(" | "),
    );
    out.push('\n');
    out.push_str(&w.iter().map(|n| "-".repeat(*n)).collect::<Vec<_>>().join("-+-"));
    for row in table {
        out.push('\n');
        out.push_str(
            &row.iter()
                .enumerate()
                .map(|(i, c)| pad(c, *w.get(i).unwrap_or(&0)))
                .collect::<Vec<_>>()
                .join(" | "),
        );
    }
    out.push_str(&format!(
        "\n({} row{})",
        table.len(),
        if table.len() == 1 { "" } else { "s" }
    ));
    out
}

/// Decode a projection result (`SELECT c1, c2 …`) — `[u32 rowlen][row]*`
/// where each row is the projected columns' bare fixed-width bytes in
/// `cols` order — against the table's wire schema (`DESCRIBE` output) and
/// render an aligned table. `None` on any mismatch (caller falls back).
pub fn render_projection(
    typedef: &[u8],
    cols: &[String],
    rows: &[u8],
) -> Option<String> {
    let (name, fields) = kessel_catalog::decode_type_def(typedef)?;
    let ot = kessel_catalog::ObjectType::from_def(name, fields);
    // Resolve each requested column to its kind+width (unknown ⇒ None).
    let mut spec: Vec<(kessel_catalog::FieldKind, usize)> =
        Vec::with_capacity(cols.len());
    for c in cols {
        let f = ot.fields.iter().find(|f| f.name.eq_ignore_ascii_case(c))?;
        spec.push((f.kind, f.kind.width() as usize));
    }
    let rowlen: usize = spec.iter().map(|(_, w)| *w).sum();
    let mut table: Vec<Vec<String>> = Vec::new();
    let mut p = 0usize;
    while p + 4 <= rows.len() {
        let len = u32::from_le_bytes(rows[p..p + 4].try_into().ok()?) as usize;
        p += 4;
        let body = rows.get(p..p + len)?;
        p += len;
        if len != rowlen {
            return None; // shape doesn't match the projection
        }
        let mut off = 0usize;
        let mut cells = Vec::with_capacity(spec.len());
        for (kind, w) in &spec {
            let raw = body.get(off..off + *w)?;
            off += *w;
            cells.push(format_value(&kessel_codec::value_from_raw(*kind, raw)));
        }
        table.push(cells);
    }
    if p != rows.len() {
        return None;
    }
    Some(render_table(cols, &table))
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
        let stream = TcpStream::connect(addr)?;
        // Disable Nagle: requests are small and synchronous, so Nagle +
        // delayed-ACK adds ~40 ms latency per round-trip on Linux/EC2.
        let _ = stream.set_nodelay(true);
        Ok(Client { stream })
    }

    /// Connect and authenticate with a shared-secret token (the server's
    /// `ServerConfig.token`). Fails with `PermissionDenied` if rejected.
    pub fn connect_authed(addr: impl ToSocketAddrs, token: &[u8]) -> io::Result<Self> {
        let mut stream = TcpStream::connect(addr)?;
        let _ = stream.set_nodelay(true);
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
            let _ = s.set_nodelay(true);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_result_is_readable_for_every_variant() {
        assert_eq!(format_result(&OpResult::Ok), "OK");
        assert!(format_result(&OpResult::TypeCreated(3)).contains("type_id=3"));
        assert!(format_result(&OpResult::Exists).starts_with("EXISTS"));
        assert_eq!(format_result(&OpResult::NotFound), "NOT FOUND");
        assert!(format_result(&OpResult::Constraint("UNIQUE x".into()))
            .contains("UNIQUE x"));
        assert!(format_result(&OpResult::SchemaError("bad".into())).contains("bad"));
        assert!(format_result(&OpResult::Unavailable).contains("primary"));
        assert!(format_result(&OpResult::Unauthorized).contains("token"));
        // scalar reply (aggregate = 16-byte i128)
        assert!(format_result(&OpResult::Got(1049i128.to_le_bytes().to_vec()))
            .contains("= 1049"));
        // opaque rows
        assert!(format_result(&OpResult::Got(vec![0u8; 40])).contains("40 bytes"));
        // never panics, always non-empty
        for r in [
            OpResult::Ok,
            OpResult::Got(vec![]),
            OpResult::Got(vec![1, 2, 3]),
        ] {
            assert!(!format_result(&r).is_empty());
        }
    }

    #[test]
    fn render_projection_decodes_column_oriented_rows() {
        use kessel_catalog::{encode_type_def, Field, FieldKind};
        let fields = vec![
            Field { field_id: 1, name: "owner".into(), kind: FieldKind::U32, nullable: false },
            Field { field_id: 2, name: "bal".into(), kind: FieldKind::I64, nullable: false },
            Field { field_id: 3, name: "tag".into(), kind: FieldKind::U16, nullable: false },
        ];
        let typedef = encode_type_def("acct", &fields);
        // Projection `SELECT owner, bal` ⇒ each row = u32 LE ++ i64 LE,
        // length-prefixed.
        let mut rows = Vec::new();
        for (o, b) in [(100u32, 50i64), (7, -3)] {
            let mut row = o.to_le_bytes().to_vec();
            row.extend_from_slice(&b.to_le_bytes());
            rows.extend_from_slice(&(row.len() as u32).to_le_bytes());
            rows.extend_from_slice(&row);
        }
        let cols = vec!["owner".to_string(), "bal".to_string()];
        let out = render_projection(&typedef, &cols, &rows).expect("decodes");
        assert!(out.contains("owner") && out.contains("bal"));
        assert!(out.contains("100") && out.contains("-3"), "{out}");
        assert!(out.contains("(2 rows)"), "{out}");

        // Unknown projected column ⇒ None (CLI falls back to bytes).
        assert!(render_projection(&typedef, &["nope".into()], &rows).is_none());
        // Wrong row shape ⇒ None.
        assert!(
            render_projection(&typedef, &["owner".into()], &rows).is_none(),
            "rowlen mismatch must reject"
        );
    }

    #[test]
    fn render_rows_decodes_and_aligns() {
        use kessel_catalog::{encode_type_def, Field, FieldKind};
        let fields = vec![
            Field { field_id: 1, name: "owner".into(), kind: FieldKind::U32, nullable: false },
            Field { field_id: 2, name: "bal".into(), kind: FieldKind::I64, nullable: false },
        ];
        let typedef = encode_type_def("acct", &fields);
        let ot = kessel_catalog::ObjectType::from_def("acct".into(), fields);

        let mut rows = Vec::new();
        for (o, b) in [(100u128, 50i128), (7, -3)] {
            let rec = kessel_codec::encode(
                &ot,
                &[kessel_codec::Value::Uint(o), kessel_codec::Value::Int(b)],
            )
            .unwrap();
            rows.extend_from_slice(&(rec.len() as u32).to_le_bytes());
            rows.extend_from_slice(&rec);
        }

        let out = render_rows(&typedef, &rows).expect("decodes");
        assert!(out.contains("owner"), "header: {out}");
        assert!(out.contains("bal"));
        assert!(out.contains("100"));
        assert!(out.contains("-3"), "signed value: {out}");
        assert!(out.contains("(2 rows)"), "row count: {out}");

        // Malformed → None (CLI then falls back to opaque bytes).
        assert!(render_rows(&typedef, &[0xFF, 0xFF, 0xFF, 0xFF, 1]).is_none());
        assert!(render_rows(b"not a typedef", &rows).is_none());
        // Zero rows still renders a header + "(0 rows)".
        let empty = render_rows(&typedef, &[]).expect("header only");
        assert!(empty.contains("owner") && empty.contains("(0 rows)"));

        // Single BARE record (the `SELECT * ... ID <n>` fast path shape).
        let one = kessel_codec::encode(
            &ot,
            &[kessel_codec::Value::Uint(42), kessel_codec::Value::Int(9)],
        )
        .unwrap();
        let r = render_rows(&typedef, &one).expect("single record decodes");
        assert!(r.contains("42") && r.contains("(1 row)"), "single: {r}");
    }
}
