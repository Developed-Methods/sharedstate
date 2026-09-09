#![cfg(feature = "experimental-v4")]
use serde::{Deserialize, Serialize};
use sharedstate::{
    transport::simulated::SimulatedNet,
    v4::{Node, NodeConfig, Operation, OperationId, ReplicatedState},
};
use std::{collections::BTreeMap, time::Duration};
use tokio::time::Instant;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Panicking(u64);
impl ReplicatedState for Panicking {
    type Command = u64;
    type Result = ();
    const SCHEMA_VERSION: u32 = 1;
    fn apply(&mut self, _: u64) {
        self.0 += 1;
        panic!("injected deterministic apply failure");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t17_apply_panic_makes_readiness_false_and_stops_acknowledgements() {
    let directory = tempfile::tempdir().unwrap();
    let cluster = uuid::Uuid::new_v4();
    let network = SimulatedNet::new();
    let mut nodes = BTreeMap::new();
    for id in 1..=3 {
        let node = Node::open(
            NodeConfig::new(directory.path().join(format!("{id}.db")), cluster, id),
            network.start_io(id).await,
            Panicking::default(),
        )
        .await
        .unwrap();
        nodes.insert(id, node);
    }
    nodes[&1]
        .bootstrap(BTreeMap::from([(1, 1), (2, 2), (3, 3)]))
        .await
        .unwrap();
    let leader = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (id, node) in &nodes {
                if node.status().leader == Some(*id) && node.status().ready {
                    return *id;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let result = nodes[&leader]
        .submit(
            Operation {
                id: OperationId {
                    client_id: uuid::Uuid::new_v4(),
                    sequence: 1,
                },
                command: 1,
            },
            Instant::now() + Duration::from_secs(3),
        )
        .await;
    assert!(result.is_err());
    tokio::time::timeout(Duration::from_secs(3), async {
        while nodes[&leader].status().ready {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(nodes[&leader].status().failure.is_some());
    assert_eq!(nodes[&leader].read_snapshot().state.0, 0, "partial apply must never be published");
    for (_, node) in nodes {
        let _ = node.shutdown(Instant::now() + Duration::from_secs(3)).await;
    }
}
