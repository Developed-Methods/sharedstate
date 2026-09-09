#![cfg(feature = "experimental-v4")]
use serde::{Deserialize, Serialize};
use sharedstate::{
    transport::{simulated::SimulatedNet, traits::SyncIO},
    v4::{Node, NodeConfig, ReplicatedState},
};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Counter(u64);
impl ReplicatedState for Counter {
    type Command = u64;
    type Result = u64;
    const SCHEMA_VERSION: u32 = 1;
    fn apply(&mut self, delta: u64) -> u64 {
        self.0 += delta;
        self.0
    }
}

#[tokio::test]
async fn t16_shutdown_closes_stalled_handshakes() {
    let dir = tempfile::tempdir().unwrap();
    let net = SimulatedNet::new();
    let io = net.start_io(1).await;
    let remote = net.start_io(2).await;
    let node = Node::open(NodeConfig::new(dir.path().join("raft.db"), uuid::Uuid::new_v4(), 1), io, Counter::default())
        .await
        .unwrap();
    let connection = remote.connect(&1).await.unwrap();
    tokio::task::yield_now().await;
    node.shutdown(Instant::now() + Duration::from_secs(1)).await.unwrap();
    use tokio::io::AsyncReadExt;
    let mut reader = connection.read;
    let mut byte = [0];
    let result = tokio::time::timeout(Duration::from_secs(1), reader.read(&mut byte))
        .await
        .unwrap();
    assert!(matches!(result, Ok(0) | Err(_)));
}
