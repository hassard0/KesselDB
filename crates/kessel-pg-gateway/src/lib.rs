//! SP-PG — PostgreSQL wire protocol support for KesselDB (Frontend/Backend
//! Protocol v3.0, `psql` + JDBC + libpq + pgx etc. compatibility).
//!
//! **T1 status (current):** design spec + scaffolding only. The
//! `server::accept` entry point returns `Err(PgError::NotYetImplemented)`;
//! no bytes flow yet. T2 ships the startup handshake + SCRAM-SHA-256
//! auth. The 18-slice V1 plan is in the companion design spec.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`
//!
//! ## Task decomposition (mirrors spec §10)
//!
//! - **T1** (this commit) — design spec + scaffold (crate, `proto.rs`
//!   message-type tags + protocol constants, `server.rs` placeholder
//!   accept returning `Err(PgError::NotYetImplemented)`, locked
//!   constants, 3-8 KATs locking spec invariants)
//! - **T2** — startup handshake + SCRAM-SHA-256 auth (StartupMessage
//!   parser, ParameterStatus / BackendKeyData / ReadyForQuery emit,
//!   SCRAM 4-round-trip state machine, PBKDF2-HMAC-SHA-256 added to
//!   `kessel-crypto`, Bearer-token bridge per spec §3.4)
//! - **T3** — Simple Query: `Q` message parser, dispatch into
//!   `EngineApply::apply_sql`
//! - **T4** — PG type-OID mapping table + text-format renderer
//! - **T5** — RowDescription + DataRow encoders (per-row streaming)
//! - **T6** — CommandComplete + ReadyForQuery encoders
//! - **T7** — ErrorResponse encoder + OpResult→SQLSTATE map
//! - **T8** — SELECT end-to-end (schema lookup via new
//!   `EngineApply::describe_table` method)
//! - **T9** — INSERT / UPDATE / DELETE end-to-end
//! - **T10** — psql compatibility hand-test + synthetic-peer KAT
//! - **T11** — pgcli / DBeaver / JDBC compatibility smoke
//! - **T12** — `kesseldb-server` `pg-gateway` feature flag + listener wire-up
//! - **T13** — Bounded connection cap
//! - **T14** — Pentest sweep (10+ adversarial inputs)
//! - **T15** — Per-connection reader/writer-thread split
//! - **T16** — Idle timeout + graceful Terminate handling
//! - **T17** — Scatter-scan integration (cross-shard SELECT)
//! - **T18** — Docs (ARCHITECTURE, USAGE, README)
//!
//! V2 follow-ups (T19+): Extended Query, binary format, `pg_catalog`,
//! RETURNING, COPY, CancelRequest, GUC, TLS, MD5 fallback.
//!
//! ## Zero-dep stance
//!
//! `std::net::TcpStream`, `std::thread`, `std::sync::mpsc` only. SHA-256
//! + HMAC-SHA-256 + (T2-incoming) PBKDF2 + base64 come from
//! `kessel-crypto` (workspace, zero external dep). No tokio-postgres,
//! no pgwire crate, no async runtime — same shape as `kessel-http-
//! gateway`.
//!
//! ## Listener model
//!
//! Sibling listener on a dedicated TCP port (default 5432). Per-
//! connection `std::thread`. Connection cap defaults to
//! `DEFAULT_MAX_PG_CONNS = 256` (smaller than HTTP gateway's 1024
//! because PG clients hold connections longer). The PG and HTTP
//! caps are SEPARATE — a misbehaving pgcli cannot starve HTTP
//! clients.

#![forbid(unsafe_code)]
#![allow(dead_code)]

pub mod auth;
pub mod proto;
pub mod query;
pub mod server;
pub mod startup;

pub use server::{accept, AcceptedSession, PgError};

/// Spec §8.1: default TCP port for the PostgreSQL Frontend/Backend
/// protocol. Standard libpq/psql/JDBC default. Operators MAY override
/// via `PgGatewayConfig.listen_addr`. Locked here so the value can be
/// referenced by the spec + tests without magic numbers.
pub const PG_GATEWAY_DEFAULT_PORT: u16 = 5432;

/// Spec §8.1: per-PG-connection bounded send queue depth. Chosen
/// deeper than SP-WS's `WS_SEND_QUEUE_BOUND=16` because PG streams
/// `DataRow` per result row — a SELECT returning 10K rows must not
/// deadlock if the OS write buffer is slow to drain. T15 wires this;
/// today it's a locked constant for forward reference.
pub const PG_SEND_QUEUE_BOUND: usize = 64;

/// Spec §8.1: per-PG-listener default connection cap. Smaller than
/// `kessel-http-gateway::DEFAULT_MAX_CONNS=1024` because PG clients
/// hold connections longer (typical ORM connection pool: 10-50 per
/// app instance). 256 is enough for typical workloads while keeping
/// per-thread overhead bounded. T13 wires this.
pub const DEFAULT_MAX_PG_CONNS: usize = 256;

/// Spec §8.1: default idle-connection timeout (seconds). A connection
/// that hasn't sent any client message for this long is closed. 600s
/// is the libpq default `tcp_user_timeout` + a margin. T16 wires this.
pub const PG_DEFAULT_IDLE_TIMEOUT_SECS: u64 = 600;

/// Spec §3.1: cap on any single inbound PG message length BEFORE
/// allocation. A client claiming a 1 GiB message gets a clean
/// rejection via `08P01` protocol_violation, never
/// `Vec::with_capacity(1 GiB)`. Matches `kessel-http-gateway::ws::frame
/// ::MAX_PAYLOAD` for operational uniformity. T3 enforces.
pub const PG_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Spec §3.3: PBKDF2 iteration count for SCRAM-SHA-256 password
/// salting. PG's `password_encryption = scram-sha-256` default since
/// PG 10. T2 uses this in the SCRAM state machine. (V1 threat model
/// doesn't justify a higher count — the Bearer token is high-entropy
/// by construction; PBKDF2 here is "look like PG" rather than
/// "harden a low-entropy password".)
pub const PG_DEFAULT_SCRAM_ITERATIONS: u32 = 4096;

/// Spec §3.3 / §6: the only SASL mechanism V1 advertises. Listed by
/// the server in `AuthenticationSASL`; the client picks it and sends
/// its `client_first` message back. PG 10+ default. T2 uses this.
pub const SUPPORTED_SASL_MECH: &str = "SCRAM-SHA-256";
