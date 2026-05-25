//! HTTP/1.1 request parser (request line, headers, body). T2 and T3 fill
//! this module. Hand-rolled per RFC 9112; no `httparse`/`hyper`.

#![allow(dead_code)]
