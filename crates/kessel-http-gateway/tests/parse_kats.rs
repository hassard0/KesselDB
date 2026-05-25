//! Hand-built RFC 9112 byte KATs for the HTTP/1.1 request parser. Every
//! KAT is derived independently from the RFC and serves as the spec-compliance
//! oracle for `parse.rs` — the spec-compliance reviewer re-derives each one.

use kessel_http_gateway::parse::{parse_request, ParseError, Method};

#[test]
fn kat_simple_get_health() {
    // Hand-derived from RFC 9112 §3 + §5. Request line + Host + blank.
    let bytes = b"GET /v1/health HTTP/1.1\r\nHost: localhost:6789\r\n\r\n";
    let req = parse_request(bytes).expect("well-formed GET parses");
    assert_eq!(req.method, Method::Get);
    assert_eq!(req.path, "/v1/health");
    assert_eq!(req.host, "localhost:6789");
    assert!(req.body.is_empty());
    assert_eq!(req.consumed, bytes.len());
}

#[test]
fn kat_simple_post_sql_content_length() {
    // Hand-derived: request line + Host + Content-Length + Content-Type +
    // blank + body.
    let body = b"SELECT 1";
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHost: localhost:6789\r\n\
                  Content-Type: text/plain\r\nContent-Length: 8\r\n\r\nSELECT 1";
    let req = parse_request(bytes).expect("well-formed POST parses");
    assert_eq!(req.method, Method::Post);
    assert_eq!(req.path, "/v1/sql");
    assert_eq!(req.body, body);
    assert_eq!(req.content_type.as_deref(), Some("text/plain"));
    assert_eq!(req.consumed, bytes.len());
}

#[test]
fn kat_post_op_binary_content_type() {
    let body = vec![0x01, 0x02, 0x03];
    let mut bytes = b"POST /v1/op HTTP/1.1\r\nHost: h\r\n\
                      Content-Type: application/x-kessel-op\r\n\
                      Content-Length: 3\r\n\r\n".to_vec();
    bytes.extend_from_slice(&body);
    let req = parse_request(&bytes).expect("binary body parses");
    assert_eq!(req.body, body.as_slice());
    assert_eq!(req.content_type.as_deref(),
               Some("application/x-kessel-op"));
}

#[test]
fn kat_rejects_missing_host() {
    let bytes = b"GET /v1/health HTTP/1.1\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::MissingHost), "got {:?}", err);
}

#[test]
fn kat_rejects_ipv6_literal_host() {
    let bytes = b"GET /v1/health HTTP/1.1\r\nHost: [::1]:6789\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::Ipv6LiteralHost), "got {:?}", err);
}

#[test]
fn kat_rejects_unknown_method() {
    let bytes = b"DELETE /v1/sql HTTP/1.1\r\nHost: h\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::MethodNotAllowed), "got {:?}", err);
}

#[test]
fn kat_rejects_unknown_path() {
    let bytes = b"GET /v2/sql HTTP/1.1\r\nHost: h\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::NotFound), "got {:?}", err);
}

#[test]
fn kat_rejects_bad_request_line_no_version() {
    let bytes = b"GET /v1/health\r\nHost: h\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::BadRequestLine), "got {:?}", err);
}

#[test]
fn kat_rejects_http_2_0() {
    let bytes = b"GET /v1/health HTTP/2.0\r\nHost: h\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::BadRequestLine), "got {:?}", err);
}

#[test]
fn kat_post_missing_content_length() {
    // RFC 9112 §6.3 — POST with no body framing → 411 Length Required.
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                  Content-Type: text/plain\r\n\r\nSELECT 1";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::LengthRequired), "got {:?}", err);
}

#[test]
fn kat_content_length_lies_short() {
    // Declared 10 bytes, only 3 delivered before \r\n\r\n ends the input.
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                  Content-Type: text/plain\r\nContent-Length: 10\r\n\r\nabc";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::ShortBody), "got {:?}", err);
}

#[test]
fn kat_content_length_non_decimal() {
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                  Content-Type: text/plain\r\nContent-Length: abc\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::BadHeaderValue(_)), "got {:?}", err);
}

#[test]
fn kat_headers_case_insensitive() {
    // HOST in upper-case, content-length in mixed-case (RFC 9110 §5.1
    // header names are case-insensitive).
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHOST: h\r\n\
                  content-Type: text/plain\r\nContent-LENGTH: 0\r\n\r\n";
    let req = parse_request(bytes).expect("case-insensitive headers parse");
    assert_eq!(req.host, "h");
}

#[test]
fn kat_no_header_terminator() {
    let bytes = b"GET /v1/health HTTP/1.1\r\nHost: h\r\nNo-Terminator: ";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::NoHeaderTerminator), "got {:?}", err);
}

#[test]
fn kat_content_type_with_charset() {
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                  Content-Type: text/plain; charset=utf-8\r\n\
                  Content-Length: 0\r\n\r\n";
    let req = parse_request(bytes).expect("Content-Type with charset parses");
    // Only the media-type portion is returned; the charset suffix is dropped.
    assert_eq!(req.content_type.as_deref(), Some("text/plain"));
}
