//! Synchronize deterministic state machines across a cluster.
//!
//! The crate is organized into focused layers:
//! state management, protocol framing, transport adapters, cluster
//! coordination, and test orchestration utilities.

pub mod cluster;
pub mod net;
pub mod protocol;
pub mod shared;
pub mod state;
pub mod testing;
pub mod transport;

pub mod test_orchestrator;

mod utils;

// #[cfg(test)]
// mod testing;
