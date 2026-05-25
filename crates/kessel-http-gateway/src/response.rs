//! HTTP/1.1 response writer. One free function per response shape so the
//! routes module reads top-to-bottom with no hidden state. Every response
//! sends `Connection: close` — V1 has no keep-alive (spec §4.2).

use std::io::Write;

/// CRLF — kept inline for visual symmetry with RFC 9112.
const CRLF: &[u8] = b"\r\n";

/// Write a JSON response. `status` is e.g. (200, "OK"); `body_json` is the
/// JSON string from `format_result_json` (or hand-built error JSON). The
/// body is always UTF-8.
pub fn write_json<W: Write>(
    w: &mut W,
    status: (u16, &'static str),
    body_json: &str,
) -> std::io::Result<()> {
    let body = body_json.as_bytes();
    write!(w, "HTTP/1.1 {} {}\r\n", status.0, status.1)?;
    w.write_all(b"Content-Type: application/json; charset=utf-8\r\n")?;
    write!(w, "Content-Length: {}\r\n", body.len())?;
    w.write_all(b"Connection: close\r\n")?;
    w.write_all(b"Server: kesseldb/0\r\n")?;
    w.write_all(CRLF)?;
    w.write_all(body)?;
    Ok(())
}

/// Write a Prometheus text-format response (text/plain; version=0.0.4).
pub fn write_prometheus<W: Write>(
    w: &mut W,
    body: &str,
) -> std::io::Result<()> {
    let body = body.as_bytes();
    w.write_all(b"HTTP/1.1 200 OK\r\n")?;
    w.write_all(b"Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n")?;
    write!(w, "Content-Length: {}\r\n", body.len())?;
    w.write_all(b"Connection: close\r\n")?;
    w.write_all(b"Server: kesseldb/0\r\n")?;
    w.write_all(CRLF)?;
    w.write_all(body)?;
    Ok(())
}

/// JSON error helper — wraps the body in `{"status":"<semantic>","message":"…"}`
/// and writes with the chosen HTTP status.
pub fn write_error_json<W: Write>(
    w: &mut W,
    status: (u16, &'static str),
    semantic: &str,
    message: &str,
) -> std::io::Result<()> {
    let escaped = json_escape(message);
    let body = format!(r#"{{"status":"{semantic}","message":"{escaped}"}}"#);
    write_json(w, status, &body)
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}
