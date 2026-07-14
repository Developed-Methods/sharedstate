//! Synchronize deterministic state machines across a cluster.
//!
//! The crate is organized into focused layers: state management, protocol
//! framing, transport adapters, and cluster coordination.
//!
//! Reads are local and low latency: a handle observes this node's current
//! lineage immediately. During partitions or leader changes, writes accepted
//! on a losing lineage may later be discarded when the node resets to the
//! surviving leader. Treat local reads as lineage-consistent, not as proof that
//! every observed write is quorum durable.
//!
//! State is kept in memory unless the application checkpoints it. A cluster can
//! recover from node failures while at least one up-to-date node remains, but a
//! simultaneous restart of every voter loses state unless callers periodically
//! persist [`SharedState::snapshot`] and restart with
//! [`SharedState::start_recoverable`].
//!
//! Membership is gossiped in memory and voter records are not removed by a
//! replicated membership-change protocol. Use stable peer identities for voters
//! rather than ephemeral addresses.

pub mod cluster;
pub mod protocol;
pub mod service;
pub mod state;
pub mod transport;

mod utils;

pub use service::{SharedState, SharedStateConfig, SharedStateRecoverableConfig, SharedStateSettings};
