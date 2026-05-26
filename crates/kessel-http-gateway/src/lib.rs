//! KesselDB HTTP/1.1 wire gateway (SP141).
//!
//! Opt-in via the `kesseldb-server` `http-gateway` cargo feature. Translates
//! HTTP/1.1 requests on a sibling listener into the existing Op-apply pipeline
//! and emits `kessel_client::format_result_json` responses. The binary wire
//! protocol is byte-untouched.
//!
//! Zero external (non-workspace) dependencies — `std::net::TcpListener`,
//! `std::thread`, and a hand-rolled HTTP/1.1 parser. See
//! `docs/superpowers/specs/2026-05-24-kesseldb-http-gateway-design.md`.

#![forbid(unsafe_code)]
#![allow(dead_code)]

pub mod engine;
pub mod metrics_writer;
pub mod parse;
pub mod response;
pub mod routes;
pub mod server;

pub use engine::{EngineApply, HealthSnapshot, HttpRequestCounter, HttpRequestCountersStatic, MetricsSnapshot, OpKindCounter};
pub use server::{serve, serve_tls, TlsAccept, DEFAULT_MAX_CONNS};
