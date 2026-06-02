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
        OpResult::SchemaError(m) => {
            // The server-side `apply_one` paths uniformly prefix
            // SQL-compile errors with `"sql: "`. Strip it on the way out
            // so the user sees the friendly message directly
            // (`ERROR  unknown table \`foo\``) rather than the
            // double-namespaced `ERROR  sql: unknown table \`foo\``.
            let body = m.strip_prefix("sql: ").unwrap_or(m);
            format!("ERROR  {body}")
        }
        OpResult::Unavailable => {
            "UNAVAILABLE  (this node is not the active primary — connect with a \
             cluster address list / ClusterClient)"
                .to_string()
        }
        OpResult::Unauthorized => {
            "UNAUTHORIZED  (auth failed — does your --token / \
             $KESSELDB_TOKEN match the server's KESSELDB_TOKEN env?)"
                .to_string()
        }
        OpResult::Got(b) if b.len() == 16 => {
            // The common scalar reply (aggregate result is a 16-byte i128).
            format!("= {}  ({} bytes)", i128::from_le_bytes(b[..16].try_into().unwrap()), b.len())
        }
        OpResult::Got(b) => {
            format!("GOT  {} bytes  (use `DESCRIBE <table>` to decode rows)", b.len())
        }
        // SP112 T2: Op::CommitTx outcomes surfaced at the CLI.
        OpResult::TxCommitted { commit_opnum } => {
            format!("OK  (tx committed at opnum={commit_opnum})")
        }
        OpResult::TxAborted { reason } => {
            use kessel_proto::AbortReason;
            match reason {
                AbortReason::SnapshotOutOfRange => {
                    "ABORTED  (snapshot_opnum > commit_opnum — malformed input)".to_string()
                }
                AbortReason::WriteWriteConflict { type_id, .. } => {
                    format!(
                        "ABORTED  (write-write conflict on type_id={type_id}; \
                         retry with a fresher snapshot)"
                    )
                }
                AbortReason::StorageIo { kind } => {
                    format!("ABORTED  (storage I/O kind={kind})")
                }
                _ => "ABORTED  (unknown reason — future variant)".to_string(),
            }
        }
        // SP114 / S2.5: GC watermark advance outcomes.
        OpResult::WatermarkAdvanced { new_low_water_mark, versions_deleted, pending_txs_evicted } => {
            format!(
                "OK  (watermark advanced to {new_low_water_mark}; \
                 {versions_deleted} versions deleted; \
                 {pending_txs_evicted} pending_txs evicted)"
            )
        }
        OpResult::WatermarkRejected { reason } => {
            use kessel_proto::WatermarkRejection;
            match reason {
                WatermarkRejection::NotMonotonic { proposed, current } => {
                    format!(
                        "REJECTED  (watermark not monotonic: proposed={proposed} \
                         <= current={current})"
                    )
                }
                WatermarkRejection::AboveCommitCeiling { proposed, current_commit } => {
                    format!(
                        "REJECTED  (watermark above commit ceiling: proposed={proposed} \
                         > current_commit={current_commit})"
                    )
                }
                _ => "REJECTED  (unknown reason — future variant)".to_string(),
            }
        }
        // SP123 / S2.X: per-replica active-snapshot reports.
        OpResult::ActiveSnapshotReported { replica_id, accepted_min } => {
            format!("OK  (replica {replica_id} reported active_snapshot={accepted_min})")
        }
        OpResult::ActiveSnapshotRejected { replica_id, previous_min, proposed } => {
            format!(
                "REJECTED  (replica {replica_id} non-monotonic snapshot: \
                 proposed={proposed} < previous_min={previous_min})"
            )
        }
    }
}

/// Minimal RFC-8259 string escaper (zero-dep). Used by the `--json`
/// output mode so agents/scripts get machine-parseable results.
fn json_str(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('"');
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\t' => o.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                o.push_str(&format!("\\u{:04x}", c as u32))
            }
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

/// A `kessel_codec::Value` as a JSON scalar (numbers bare, blobs as
/// trimmed text or `0x…` hex strings, NULL as `null`).
fn json_value(v: &kessel_codec::Value) -> String {
    use kessel_codec::Value::*;
    match v {
        Null => "null".to_string(),
        Uint(u) => u.to_string(),
        Int(i) => i.to_string(),
        Blob(_) => json_str(&format_value(v)),
    }
}

/// Render `SELECT *` rows as a JSON array of objects keyed by column
/// name (`[{"col":val,…},…]`), decoded against the `DESCRIBE` typedef.
/// `None` on a schema/row mismatch — the caller then falls back. Pure.
pub fn render_rows_json(typedef: &[u8], rows: &[u8]) -> Option<String> {
    let (name, fields) = kessel_catalog::decode_type_def(typedef)?;
    let ot = kessel_catalog::ObjectType::from_def(name, fields);
    let names: Vec<String> =
        ot.fields.iter().map(|f| f.name.clone()).collect();
    if names.is_empty() {
        return None;
    }
    let decode_one = |rec: &[u8]| -> Option<String> {
        let vals = kessel_codec::decode(&ot, rec).ok()?;
        let mut obj = String::from("{");
        for (i, v) in vals.iter().enumerate() {
            if i > 0 {
                obj.push(',');
            }
            obj.push_str(&json_str(&names[i]));
            obj.push(':');
            obj.push_str(&json_value(v));
        }
        obj.push('}');
        Some(obj)
    };
    let mut items: Vec<String> = Vec::new();
    let mut p = 0usize;
    let mut consumed_lp = true;
    while p + 4 <= rows.len() {
        let len =
            u32::from_le_bytes(rows[p..p + 4].try_into().ok()?) as usize;
        p += 4;
        let rec = rows.get(p..p + len)?;
        p += len;
        match decode_one(rec) {
            Some(o) => items.push(o),
            None => {
                consumed_lp = false;
                break;
            }
        }
    }
    if !consumed_lp || p != rows.len() {
        // single bare record (primary-key fast path) or not length-prefixed
        if rows.is_empty() {
            return Some("[]".to_string());
        }
        let o = decode_one(rows)?;
        return Some(format!("[{o}]"));
    }
    Some(format!("[{}]", items.join(",")))
}

/// A `FieldKind` as a short, friendly SQL-ish type name for `DESCRIBE`
/// / `\d` output.
fn kind_name(k: &kessel_catalog::FieldKind) -> String {
    use kessel_catalog::FieldKind::*;
    match k {
        U8 => "U8".into(),
        U16 => "U16".into(),
        U32 => "U32".into(),
        U64 => "U64".into(),
        U128 => "U128".into(),
        I8 => "I8".into(),
        I16 => "I16".into(),
        I32 => "I32".into(),
        I64 => "I64".into(),
        I128 => "I128".into(),
        Bool => "BOOL".into(),
        Fixed { scale } => format!("FIXED(scale={scale})"),
        Char(n) => format!("CHAR({n})"),
        Bytes(n) => format!("BYTES({n})"),
        Timestamp => "TIMESTAMP".into(),
        Ref => "REF".into(),
        OverflowRef => "OVERFLOWREF".into(),
    }
}

/// Magic prefix of a self-describing typed result (SP72): the server
/// embeds the result's own column schema so the client renders any
/// shape — JOINs today, more later — with no `DESCRIBE` round-trip.
pub const TYPED_RESULT_MAGIC: &[u8; 4] = b"KTR1";

/// If `b` is a typed result (`[KTR1][u32 deflen][type def][rows…]`),
/// split it into the embedded type def and the length-prefixed row
/// bytes. `None` if it isn't one — the caller falls back. Pure.
fn split_typed_result(b: &[u8]) -> Option<(&[u8], &[u8])> {
    if b.len() < 8 || &b[..4] != TYPED_RESULT_MAGIC {
        return None;
    }
    let dl = u32::from_le_bytes(b[4..8].try_into().ok()?) as usize;
    let def = b.get(8..8 + dl)?;
    let rows = b.get(8 + dl..)?;
    Some((def, rows))
}

/// Render a self-describing typed result as an aligned table — reuses
/// the exact same decoder as whole-row `SELECT *`, so a JOIN renders
/// identically to a plain table. `None` if `b` is not a typed result.
pub fn render_typed_result(b: &[u8]) -> Option<String> {
    let (def, rows) = split_typed_result(b)?;
    render_rows(def, rows)
}

/// The JSON form of [`render_typed_result`] — `[{col:val,…},…]`.
pub fn render_typed_result_json(b: &[u8]) -> Option<String> {
    let (def, rows) = split_typed_result(b)?;
    render_rows_json(def, rows)
}

/// Decode a `DESCRIBE` typedef into a readable schema table
/// (`column | type | null`). `None` if it isn't a valid typedef — the
/// caller falls back to the byte summary. Pure and total.
pub fn render_schema(typedef: &[u8]) -> Option<String> {
    let (name, fields) = kessel_catalog::decode_type_def(typedef)?;
    let headers = vec![
        "column".to_string(),
        "type".to_string(),
        "null".to_string(),
    ];
    let table: Vec<Vec<String>> = fields
        .iter()
        .map(|f| {
            vec![
                f.name.clone(),
                kind_name(&f.kind),
                if f.nullable { "YES".into() } else { "NO".into() },
            ]
        })
        .collect();
    Some(format!("table {name}\n{}", render_table(&headers, &table)))
}

/// `DESCRIBE` typedef as JSON: `{"table":"…","columns":[{"name","type",
/// "nullable"},…]}`. `None` if not a typedef. Pure.
pub fn render_schema_json(typedef: &[u8]) -> Option<String> {
    let (name, fields) = kessel_catalog::decode_type_def(typedef)?;
    let cols: Vec<String> = fields
        .iter()
        .map(|f| {
            format!(
                r#"{{"name":{},"type":{},"nullable":{}}}"#,
                json_str(&f.name),
                json_str(&kind_name(&f.kind)),
                f.nullable
            )
        })
        .collect();
    Some(format!(
        r#"{{"status":"ok","table":{},"columns":[{}]}}"#,
        json_str(&name),
        cols.join(",")
    ))
}

/// An `OpResult` as a single JSON object — the stable machine contract
/// for `kessel --json`. Scalar/typed-row rendering is layered on top by
/// the CLI; this is the total fallback and the non-row cases. Pure.
pub fn format_result_json(r: &OpResult) -> String {
    match r {
        OpResult::Ok => r#"{"status":"ok"}"#.to_string(),
        OpResult::TypeCreated(t) => {
            format!(r#"{{"status":"ok","type_id":{t}}}"#)
        }
        OpResult::Exists => r#"{"status":"exists"}"#.to_string(),
        OpResult::NotFound => r#"{"status":"not_found"}"#.to_string(),
        OpResult::Constraint(m) => {
            format!(r#"{{"status":"constraint","message":{}}}"#, json_str(m))
        }
        OpResult::SchemaError(m) => {
            // Mirror the text path: strip the server-side `"sql: "`
            // prefix so JSON consumers get the friendly inner message
            // (e.g. ``unknown table `foo` — did you mean `food`?``).
            let body = m.strip_prefix("sql: ").unwrap_or(m);
            format!(r#"{{"status":"error","message":{}}}"#, json_str(body))
        }
        OpResult::Unavailable => {
            r#"{"status":"unavailable"}"#.to_string()
        }
        OpResult::Unauthorized => {
            r#"{"status":"unauthorized"}"#.to_string()
        }
        OpResult::Got(b) if b.len() == 16 => {
            format!(
                r#"{{"status":"ok","value":{}}}"#,
                i128::from_le_bytes(b[..16].try_into().unwrap())
            )
        }
        OpResult::Got(b) => {
            format!(r#"{{"status":"ok","bytes":{}}}"#, b.len())
        }
        // SP112 T2: Op::CommitTx JSON outcomes.
        OpResult::TxCommitted { commit_opnum } => {
            format!(r#"{{"status":"tx_committed","commit_opnum":{commit_opnum}}}"#)
        }
        OpResult::TxAborted { reason } => {
            use kessel_proto::AbortReason;
            match reason {
                AbortReason::SnapshotOutOfRange => {
                    r#"{"status":"tx_aborted","reason":"snapshot_out_of_range"}"#.to_string()
                }
                AbortReason::WriteWriteConflict { type_id, .. } => {
                    format!(
                        r#"{{"status":"tx_aborted","reason":"write_write_conflict","type_id":{type_id}}}"#
                    )
                }
                AbortReason::StorageIo { kind } => {
                    format!(r#"{{"status":"tx_aborted","reason":"storage_io","kind":{kind}}}"#)
                }
                _ => r#"{"status":"tx_aborted","reason":"unknown"}"#.to_string(),
            }
        }
        // SP114 / S2.5: GC watermark advance outcomes.
        OpResult::WatermarkAdvanced { new_low_water_mark, versions_deleted, pending_txs_evicted } => {
            format!(
                r#"{{"status":"watermark_advanced","new_low_water_mark":{new_low_water_mark},"versions_deleted":{versions_deleted},"pending_txs_evicted":{pending_txs_evicted}}}"#
            )
        }
        OpResult::WatermarkRejected { reason } => {
            use kessel_proto::WatermarkRejection;
            match reason {
                WatermarkRejection::NotMonotonic { proposed, current } => {
                    format!(
                        r#"{{"status":"watermark_rejected","reason":"not_monotonic","proposed":{proposed},"current":{current}}}"#
                    )
                }
                WatermarkRejection::AboveCommitCeiling { proposed, current_commit } => {
                    format!(
                        r#"{{"status":"watermark_rejected","reason":"above_commit_ceiling","proposed":{proposed},"current_commit":{current_commit}}}"#
                    )
                }
                _ => r#"{"status":"watermark_rejected","reason":"unknown"}"#.to_string(),
            }
        }
        // SP123 / S2.X: per-replica active-snapshot reports (JSON form).
        OpResult::ActiveSnapshotReported { replica_id, accepted_min } => {
            format!(
                r#"{{"status":"active_snapshot_reported","replica_id":{replica_id},"accepted_min":{accepted_min}}}"#
            )
        }
        OpResult::ActiveSnapshotRejected { replica_id, previous_min, proposed } => {
            format!(
                r#"{{"status":"active_snapshot_rejected","replica_id":{replica_id},"previous_min":{previous_min},"proposed":{proposed}}}"#
            )
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

/// Pipeline tag (mirrors `kesseldb_server::PIPELINE_TAG`).
pub const PIPELINE_TAG: u8 = 0xF8;

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

    /// Pipeline a batch of SQL statements in ONE round-trip. Each runs
    /// independently (this is **not** a transaction — for atomicity use
    /// `BEGIN`/`COMMIT`); the win is that the whole batch costs a single
    /// network round-trip and lands in one server group-commit fsync.
    /// Returns one `OpResult` per statement, in order. A pipelined
    /// statement behaves exactly as if sent alone via [`Client::sql`].
    pub fn pipeline(&mut self, stmts: &[&str]) -> io::Result<Vec<OpResult>> {
        let mut frame = vec![PIPELINE_TAG];
        frame.extend_from_slice(&(stmts.len() as u32).to_le_bytes());
        for s in stmts {
            // each member is an ordinary `[0xFE] ++ SQL` inner frame
            let mut inner = Vec::with_capacity(s.len() + 1);
            inner.push(0xFE);
            inner.extend_from_slice(s.as_bytes());
            frame.extend_from_slice(&(inner.len() as u32).to_le_bytes());
            frame.extend_from_slice(&inner);
        }
        write_frame(&mut self.stream, &frame)?;
        let resp = read_frame(&mut self.stream)?;
        let bad =
            || io::Error::new(io::ErrorKind::InvalidData, "bad pipeline reply");
        match OpResult::decode(&resp).ok_or_else(bad)? {
            OpResult::Got(b) => {
                let cnt = u32::from_le_bytes(
                    b.get(0..4).ok_or_else(bad)?.try_into().unwrap(),
                ) as usize;
                let mut p = 4usize;
                let mut out = Vec::with_capacity(cnt);
                for _ in 0..cnt {
                    let l = u32::from_le_bytes(
                        b.get(p..p + 4).ok_or_else(bad)?.try_into().unwrap(),
                    ) as usize;
                    p += 4;
                    let r = b.get(p..p + l).ok_or_else(bad)?;
                    p += l;
                    out.push(OpResult::decode(r).ok_or_else(bad)?);
                }
                Ok(out)
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("pipeline: unexpected reply {other:?}"),
            )),
        }
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

    /// Submit a SQL statement with failover. Wire form: `[0xFE] ++ utf8`,
    /// the same shape `Client::sql` writes — the cluster server's
    /// `apply_raw` accepts it on any node and either compiles+commits
    /// (primary) or answers `Unavailable` (backup unable to relay to a
    /// reachable primary). On `Unavailable` or a connection error we
    /// rotate to the next address and retry.
    ///
    /// Compared with [`ClusterClient::call`] this is NOT session-framed:
    /// the cluster server's session-frame path is `Op`-only (a SQL string
    /// has no `Op::decode` form). For our acceptance bar — survive a
    /// primary-kill and recover — the cluster's internal client_table
    /// dedup is sufficient because each retry happens on the surviving
    /// nodes; a write that already committed before the primary died is
    /// surfaced by the new primary's catch-up + `Got`/`Exists`/`Ok`
    /// reply on a subsequent SELECT. The only exactly-once gap is the
    /// instant the in-flight INSERT was being committed AND the primary
    /// crashed AND the reply was lost — VSR's client_table cannot dedup
    /// a fresh client_id allocated by the new primary's `apply_raw`. For
    /// strict cross-node exactly-once on writes, use `Op`-level
    /// `call(&Op)` instead (session-framed). This is documented at
    /// T3 in `2026-06-02-kesseldb-spcloudcluster-design.md`.
    pub fn sql(&mut self, sql: &str) -> io::Result<OpResult> {
        let mut frame = Vec::with_capacity(sql.len() + 1);
        frame.push(0xFE);
        frame.extend_from_slice(sql.as_bytes());
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
        assert!(format_result(&OpResult::Got(1049i128.to_le_bytes().to_vec().into()))
            .contains("= 1049"));
        // opaque rows
        assert!(format_result(&OpResult::Got(vec![0u8; 40].into())).contains("40 bytes"));
        // never panics, always non-empty
        for r in [
            OpResult::Ok,
            OpResult::Got(Vec::<u8>::new().into()),
            OpResult::Got(vec![1, 2, 3].into()),
        ] {
            assert!(!format_result(&r).is_empty());
        }
    }

    #[test]
    fn json_output_is_well_formed_and_total() {
        // Every non-row variant maps to a stable JSON object.
        assert_eq!(format_result_json(&OpResult::Ok), r#"{"status":"ok"}"#);
        assert_eq!(
            format_result_json(&OpResult::TypeCreated(3)),
            r#"{"status":"ok","type_id":3}"#
        );
        assert_eq!(
            format_result_json(&OpResult::NotFound),
            r#"{"status":"not_found"}"#
        );
        assert_eq!(
            format_result_json(&OpResult::Got(1049i128.to_le_bytes().to_vec().into())),
            r#"{"status":"ok","value":1049}"#
        );
        // Error messages are JSON-escaped, never raw.
        let j = format_result_json(&OpResult::SchemaError(
            "bad \"quote\"\n".into(),
        ));
        assert!(j.contains(r#"\"quote\""#) && j.contains(r#"\n"#), "{j}");
        assert!(!j.contains('\n'), "control chars must be escaped: {j}");

        // Typed rows → JSON array of objects keyed by column name.
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
        let j = render_rows_json(&typedef, &rows).expect("json rows");
        assert_eq!(
            j,
            r#"[{"owner":100,"bal":50},{"owner":7,"bal":-3}]"#,
            "compact, ordered, machine-parseable: {j}"
        );
        // Zero rows is a valid empty array, not None.
        assert_eq!(render_rows_json(&typedef, &[]).as_deref(), Some("[]"));
    }

    #[test]
    fn typed_result_renders_generically_text_and_json() {
        use kessel_catalog::{encode_type_def, Field, FieldKind};
        // A synthetic JOIN result: combined schema usr.uid + ord.amt.
        let combined = vec![
            Field { field_id: 0, name: "usr.uid".into(), kind: FieldKind::U32, nullable: false },
            Field { field_id: 1, name: "ord.amt".into(), kind: FieldKind::U32, nullable: false },
        ];
        let cot = kessel_catalog::ObjectType::from_def(
            "usr+ord".into(),
            combined.clone(),
        );
        let typedef = encode_type_def("usr+ord", &combined);
        let mut payload = Vec::new();
        payload.extend_from_slice(TYPED_RESULT_MAGIC);
        payload.extend_from_slice(&(typedef.len() as u32).to_le_bytes());
        payload.extend_from_slice(&typedef);
        for (u, a) in [(1u128, 100u128), (1, 200), (2, 50)] {
            let rec = kessel_codec::encode(
                &cot,
                &[kessel_codec::Value::Uint(u), kessel_codec::Value::Uint(a)],
            )
            .unwrap();
            payload.extend_from_slice(&(rec.len() as u32).to_le_bytes());
            payload.extend_from_slice(&rec);
        }

        let t = render_typed_result(&payload).expect("typed text");
        assert!(t.contains("usr.uid") && t.contains("ord.amt"), "{t}");
        assert!(t.contains("100") && t.contains("200") && t.contains("50"));
        assert!(t.contains("(3 rows)"), "{t}");

        let j = render_typed_result_json(&payload).expect("typed json");
        assert_eq!(
            j,
            r#"[{"usr.uid":1,"ord.amt":100},{"usr.uid":1,"ord.amt":200},{"usr.uid":2,"ord.amt":50}]"#
        );

        // Not a typed result ⇒ None (caller falls back), never panics.
        assert!(render_typed_result(b"KTR").is_none());
        assert!(render_typed_result(&[1, 2, 3, 4, 5, 6, 7, 8]).is_none());
        assert!(render_typed_result_json(b"").is_none());
    }

    #[test]
    fn schema_rendering_is_readable_and_json() {
        use kessel_catalog::{encode_type_def, Field, FieldKind};
        let typedef = encode_type_def(
            "acct",
            &[
                Field { field_id: 1, name: "owner".into(), kind: FieldKind::U32, nullable: false },
                Field { field_id: 2, name: "note".into(), kind: FieldKind::Char(16), nullable: true },
            ],
        );
        let t = render_schema(&typedef).expect("schema text");
        assert!(t.contains("table acct"), "{t}");
        assert!(t.contains("owner") && t.contains("U32") && t.contains("NO"));
        assert!(t.contains("note") && t.contains("CHAR(16)") && t.contains("YES"));
        let j = render_schema_json(&typedef).expect("schema json");
        assert_eq!(
            j,
            r#"{"status":"ok","table":"acct","columns":[{"name":"owner","type":"U32","nullable":false},{"name":"note","type":"CHAR(16)","nullable":true}]}"#
        );
        assert!(render_schema(b"not a typedef").is_none());
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
