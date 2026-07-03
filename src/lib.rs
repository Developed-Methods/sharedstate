//! Synchronize deterministic state machines across a cluster.
//!
//! The crate is organized into focused layers: state management, protocol
//! framing, transport adapters, and cluster coordination.

pub mod cluster;
pub mod protocol;
pub mod state;
pub mod transport;

mod utils;
