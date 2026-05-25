//! TCP accept loop + per-connection thread. T4 fills this module.

#![allow(dead_code)]

use crate::engine::EngineApply;
use std::net::TcpListener;
use std::sync::Arc;

/// Public entry-point — `kesseldb-server` calls this on a dedicated thread
/// when the `http-gateway` feature is on.
pub fn serve(_listener: TcpListener, _engine: Arc<dyn EngineApply>) {
    // T4 fills the accept loop.
}
