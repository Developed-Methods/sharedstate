//! Synchronize deterministic state machines across a cluster.
//!
//! The crate is organized into focused layers: state management, protocol
//! framing, transport adapters, and cluster coordination.

pub mod cluster;
pub mod protocol;
pub mod service;
pub mod state;
pub mod transport;

mod utils;

pub use service::{SharedState, SharedStateConfig, SharedStateRecoverableConfig, SharedStateSettings};

/// Experimental durable consensus API. Release gates remain tracked in docs/upgrade/workflow.md.
#[cfg(feature = "experimental-v4")]
pub mod v4;
