//! Synchronize deterministic state machines across a cluster.
//!
//! The crate is organized into focused layers: state management, protocol
//! framing, transport adapters, and cluster coordination.

pub mod cluster;
pub mod metrics;
pub mod protocol;
pub mod service;
pub mod state;
pub mod transport;

mod utils;

pub use service::{SharedState, SharedStateConfig, SharedStateSettings};
