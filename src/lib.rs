//! Synchronize deterministic state machines across a cluster.
//!
//! The crate is organized into focused layers:
//! state management, protocol framing, transport adapters, cluster
//! coordination, and test orchestration utilities.

pub mod cluster;
pub mod protocol;
pub mod state;
pub mod testing;
pub mod transport;

pub mod new;

mod utils;
