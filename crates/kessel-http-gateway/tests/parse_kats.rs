//! Hand-built RFC 9112 byte KATs for the HTTP/1.1 request parser. Every
//! KAT is derived independently from the RFC and serves as the spec-compliance
//! oracle for `parse.rs` — the spec-compliance reviewer re-derives each one.

use kessel_http_gateway::parse::{parse_request_default as parse_request, ParseError, Method};

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
    assert_eq!(req.body.as_ref(), body);
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
    assert_eq!(req.body.as_ref(), body.as_slice());
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

// ---------------------------------------------------------------------------
// T3 KATs: chunked decode + body cap + Bearer + X-Kessel-* extractors.
// ---------------------------------------------------------------------------

use kessel_http_gateway::parse::{
    dechunk, decode_body, extract_bearer, extract_client_id, extract_req_seq,
};

#[test]
fn kat_chunked_simple() {
    // RFC 9112 §7.1 — one chunk then terminator. "Hello" = 5 bytes; chunk
    // size in hex. `dechunk` reports the full byte count consumed including
    // the trailer-section CRLF that closes the chunk-stream.
    let body = b"5\r\nHello\r\n0\r\n\r\n";
    let (decoded, consumed) = dechunk(body, 1024).expect("simple chunked decodes");
    assert_eq!(decoded, b"Hello");
    assert_eq!(consumed, body.len());
}

#[test]
fn kat_chunked_two_chunks() {
    let body = b"5\r\nHello\r\n6\r\n World\r\n0\r\n\r\n";
    let (decoded, consumed) = dechunk(body, 1024).expect("two-chunk decodes");
    assert_eq!(decoded, b"Hello World");
    assert_eq!(consumed, body.len());
}

#[test]
fn kat_chunked_truncated_missing_crlf_after_data() {
    let body = b"5\r\nHello"; // no trailing CRLF, no 0-chunk
    let err = dechunk(body, 1024).unwrap_err();
    assert!(matches!(err, ParseError::BadChunk(_)), "got {:?}", err);
}

#[test]
fn kat_chunked_bad_size_hex() {
    let body = b"zz\r\nHello\r\n0\r\n\r\n";
    let err = dechunk(body, 1024).unwrap_err();
    assert!(matches!(err, ParseError::BadChunk(_)), "got {:?}", err);
}

#[test]
fn kat_chunked_exceeds_cap() {
    // 8 bytes total, cap 4 → BodyTooLarge.
    let body = b"5\r\nHello\r\n3\r\n!!!\r\n0\r\n\r\n";
    let err = dechunk(body, 4).unwrap_err();
    assert_eq!(err, ParseError::BodyTooLarge);
}

#[test]
fn kat_decode_body_content_length_under_cap() {
    let buf = b"hello";
    let (decoded, consumed) = decode_body(buf, Some(5), false, 1024).unwrap();
    assert_eq!(decoded.as_ref(), b"hello");
    assert_eq!(consumed, 5);
}

#[test]
fn kat_decode_body_content_length_over_cap() {
    let buf = b"hello";
    let err = decode_body(buf, Some(5), false, 4).unwrap_err();
    assert_eq!(err, ParseError::BodyTooLarge);
}

#[test]
fn kat_decode_body_both_te_and_cl_rejected() {
    let buf = b"5\r\nHello\r\n0\r\n\r\n";
    // chunked=true AND content_length=Some → ConflictingFraming.
    let err = decode_body(buf, Some(5), true, 1024).unwrap_err();
    assert_eq!(err, ParseError::ConflictingFraming);
}

#[test]
fn kat_bearer_extraction() {
    let headers = vec![
        ("Authorization".into(), "Bearer abc123def".into()),
    ];
    let tok = extract_bearer(&headers).expect("ok").expect("bearer present");
    assert_eq!(tok, b"abc123def");
}

#[test]
fn kat_bearer_missing() {
    let headers: Vec<(String, String)> = Vec::new();
    assert!(extract_bearer(&headers).expect("ok").is_none());
}

#[test]
fn kat_bearer_wrong_scheme() {
    let headers = vec![("Authorization".into(), "Basic abc".into())];
    assert!(extract_bearer(&headers).expect("ok").is_none());
}

#[test]
fn kat_bearer_scheme_case_insensitive() {
    // RFC 6750 §2.1 — scheme name is case-insensitive ("bearer" / "BEARER"
    // must both be accepted). Token bytes preserve original casing.
    for (name, scheme) in [
        ("lowercase", "bearer abc123"),
        ("UPPERCASE", "BEARER abc123"),
        ("MiXeD",     "BeArEr abc123"),
    ] {
        let headers = vec![("Authorization".into(), scheme.to_string())];
        let tok = extract_bearer(&headers).expect("ok").unwrap_or_else(||
            panic!("{name} scheme should match"));
        assert_eq!(tok, b"abc123", "{name} token preserved");
    }
}

#[test]
fn kat_client_id_32_hex() {
    let headers = vec![(
        "X-Kessel-Client-Id".into(),
        "0123456789abcdef0123456789abcdef".into(),
    )];
    let id = extract_client_id(&headers).unwrap().unwrap();
    assert_eq!(id, 0x0123456789abcdef0123456789abcdef_u128);
}

#[test]
fn kat_client_id_non_hex_rejected() {
    let headers = vec![(
        "X-Kessel-Client-Id".into(),
        "GG23456789abcdef0123456789abcdef".into(),
    )];
    let err = extract_client_id(&headers).unwrap_err();
    assert!(matches!(err, ParseError::BadHeaderValue(_)), "got {:?}", err);
}

#[test]
fn kat_client_id_wrong_length() {
    let headers = vec![("X-Kessel-Client-Id".into(), "abc".into())];
    let err = extract_client_id(&headers).unwrap_err();
    assert!(matches!(err, ParseError::BadHeaderValue(_)), "got {:?}", err);
}

#[test]
fn kat_req_seq_decimal() {
    let headers = vec![("X-Kessel-Req-Seq".into(), "42".into())];
    let seq = extract_req_seq(&headers).unwrap().unwrap();
    assert_eq!(seq, 42);
}

#[test]
fn kat_req_seq_non_decimal() {
    let headers = vec![("X-Kessel-Req-Seq".into(), "abc".into())];
    let err = extract_req_seq(&headers).unwrap_err();
    assert!(matches!(err, ParseError::BadHeaderValue(_)), "got {:?}", err);
}

// ---------------------------------------------------------------------------
// T3 fix KATs: parse_request-level smuggling primitives + exactly-once
// extractor enforcement. The 3 smuggling fixes in parse_request used to only
// have decode_body-unit coverage; these exercise the full-request path.
// ---------------------------------------------------------------------------

#[test]
fn kat_parse_request_rejects_duplicate_host() {
    // RFC 9112 §3.2 — a request MUST contain exactly one Host header.
    let bytes = b"GET /v1/health HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert_eq!(err, ParseError::DuplicateHost);
}

#[test]
fn kat_parse_request_rejects_differing_content_length() {
    // RFC 9112 §6.3.5 — multiple Content-Length values that disagree are
    // a smuggling primitive; reject before any byte is decoded.
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                  Content-Type: text/plain\r\n\
                  Content-Length: 5\r\nContent-Length: 6\r\n\r\nhello";
    let err = parse_request(bytes).unwrap_err();
    assert_eq!(err, ParseError::DuplicateContentLength);
}

#[test]
fn kat_parse_request_rejects_te_plus_cl() {
    // RFC 9112 §6.1 — Transfer-Encoding and Content-Length together is a
    // smuggling primitive; reject the request.
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                  Content-Type: text/plain\r\n\
                  Content-Length: 5\r\nTransfer-Encoding: chunked\r\n\
                  \r\n5\r\nHello\r\n0\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert_eq!(err, ParseError::ConflictingFraming);
}

#[test]
fn kat_parse_request_rejects_te_non_chunked() {
    // RFC 9112 §7 — V1 only supports the `chunked` token; anything else
    // (gzip/identity/deflate/comma-list) is rejected.
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                  Content-Type: text/plain\r\n\
                  Transfer-Encoding: gzip\r\n\r\nhello";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::UnsupportedTransferEncoding(_)),
            "got {:?}", err);
}

#[test]
fn kat_extract_bearer_rejects_duplicate_authorization() {
    // Authorization is an exactly-once header — duplicates would let a peer
    // smuggle a second token past header-aware proxies.
    let headers = vec![
        ("Authorization".into(), "Bearer a".into()),
        ("Authorization".into(), "Bearer b".into()),
    ];
    let err = extract_bearer(&headers).unwrap_err();
    assert!(matches!(err, ParseError::DuplicateHeader(_)), "got {:?}", err);
}

#[test]
fn kat_extract_client_id_rejects_duplicate() {
    let headers = vec![
        ("X-Kessel-Client-Id".into(),
         "0123456789abcdef0123456789abcdef".into()),
        ("X-Kessel-Client-Id".into(),
         "fedcba9876543210fedcba9876543210".into()),
    ];
    let err = extract_client_id(&headers).unwrap_err();
    assert!(matches!(err, ParseError::DuplicateHeader(_)), "got {:?}", err);
}

#[test]
fn kat_exactly_once_binding_dedicated_variant() {
    // SP144H T4: exactly_once_binding's both-or-neither error is now a
    // dedicated ParseError variant, not a string-grepped BadHeaderValue.
    //
    // exactly_once_binding lives in routes.rs (crate-private), so we can't
    // call it from an integration test. The dedicated variant is provable
    // by construction here; the wired-in path is covered by the existing
    // pentest_client_id_alone_400 e2e test (status 400) plus the
    // write_parse_error arm added in server.rs.
    use kessel_http_gateway::parse::{extract_client_id, extract_req_seq};

    let headers_cid_only: Vec<(String, String)> = vec![(
        "X-Kessel-Client-Id".into(),
        "0123456789abcdef0123456789abcdef".into(),
    )];
    // extract_client_id alone works
    assert!(extract_client_id(&headers_cid_only).is_ok());
    // extract_req_seq on the cid-only headers returns Ok(None) — the
    // both-or-neither check happens in routes::exactly_once_binding, not
    // in the per-header extractors.
    assert_eq!(extract_req_seq(&headers_cid_only).unwrap(), None);

    // The dedicated variant is constructible and has the expected Debug
    // shape — pins the variant name so renames break the build.
    let err = ParseError::IncompleteSessionBinding;
    assert_eq!(format!("{err:?}"), "IncompleteSessionBinding");
}
