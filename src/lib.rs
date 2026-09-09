//! Replicate deterministic state with Openraft consensus and durable SQLite storage.
//!
//! Use [`Node`] for voters and [`ReadReplica`] for nonvoting readers.
//! Submit commands with stable operation IDs and wait for committed revisions.

// Openraft storage traits require this concrete error type.
#[allow(clippy::result_large_err)]
mod machine;
mod network;
mod protocol;
mod replica;
mod service;
#[allow(clippy::result_large_err)]
mod storage;
mod tcp;
pub mod transport;
pub use replica::{ReadReplica, ReplicaStatus};
pub use tcp::TcpTransport;

pub use machine::{ReplicatedState, Revision, SnapshotHandle};
pub use service::{CommitReceipt, Node, NodeConfig, NodeStatus, Operation, OperationId, OperationStatus, SubmitError};
use std::io::Cursor;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Peer {
    pub address: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Command {
    pub operation: OperationId,
    pub payload: Vec<u8>,
    pub retire: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Response {
    pub result: Vec<u8>,
    pub error: Option<String>,
    pub revision: Option<openraft::LogId<u64>>,
}

openraft::declare_raft_types!(
    pub(crate) TypeConfig:
        D = Command,
        R = Response,
        Node = Peer,
);

pub(crate) type Raft = openraft::Raft<TypeConfig>;
pub(crate) type Entry = openraft::Entry<TypeConfig>;
pub(crate) type StorageResult<T> = Result<T, openraft::StorageError<u64>>;

pub(crate) fn storage_error(error: impl std::fmt::Display) -> openraft::StorageError<u64> {
    openraft::StorageIOError::new(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Write,
        openraft::AnyError::error(error.to_string()),
    )
    .into()
}
