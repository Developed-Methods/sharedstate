use serde::{Deserialize, Serialize};
use sharedstate::{
    Node, NodeConfig, Operation, OperationId, OperationStatus, ReplicatedState, SubmitError,
    transport::simulated::{SimulatedIo, SimulatedNet},
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::time::Instant;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Counter(u64);
impl ReplicatedState for Counter {
    type Command = u64;
    type Result = u64;
    const SCHEMA_VERSION: u32 = 1;
    fn apply(&mut self, amount: u64) -> u64 {
        self.0 += amount;
        self.0
    }
}

type Voter = Node<Counter, SimulatedIo>;
struct Cluster {
    directory: tempfile::TempDir,
    cluster: uuid::Uuid,
    network: SimulatedNet,
    nodes: BTreeMap<u64, Voter>,
}
impl Cluster {
    async fn new() -> Self {
        Self::with_initial(0).await
    }
    async fn with_initial(initial: u64) -> Self {
        let mut cluster = Self {
            directory: tempfile::tempdir().unwrap(),
            cluster: uuid::Uuid::new_v4(),
            network: SimulatedNet::new(),
            nodes: BTreeMap::new(),
        };
        for id in 1..=3 {
            cluster.start_with_initial(id, initial).await;
        }
        cluster.nodes[&1]
            .bootstrap(BTreeMap::from([(1, 1), (2, 2), (3, 3)]))
            .await
            .unwrap();
        cluster.leader(None).await;
        cluster
    }
    async fn start(&mut self, id: u64) {
        self.start_with_initial(id, 0).await;
    }
    async fn start_with_initial(&mut self, id: u64, initial: u64) {
        let io: Arc<SimulatedIo> = self.network.start_io(id).await;
        let mut config = NodeConfig::new(self.directory.path().join(format!("{id}.db")), self.cluster, id);
        config.heartbeat = Duration::from_millis(50);
        config.election_min = Duration::from_millis(150);
        config.election_max = Duration::from_millis(300);
        let node = Node::open(config, io, Counter(initial)).await.unwrap();
        self.nodes.insert(id, node);
    }
    async fn leader(&self, excluded: Option<u64>) -> u64 {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for (id, node) in &self.nodes {
                    let status = node.status();
                    if Some(*id) != excluded && status.leader == Some(*id) && status.ready {
                        return *id;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("leader elected within deadline")
    }
    async fn stop(&mut self, id: u64) {
        if let Some(node) = self.nodes.remove(&id) {
            node.shutdown(Instant::now() + Duration::from_secs(5)).await.unwrap();
        }
        self.network.stop_node(id).await;
    }
    async fn close(mut self) {
        for id in self.nodes.keys().copied().collect::<Vec<_>>() {
            self.stop(id).await;
        }
    }
}
fn operation(id: OperationId, amount: u64) -> Operation<u64> {
    Operation { id, command: amount }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t07_t08_t10_t14_receipts_dedup_failover_restart_and_retained_reader() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(None).await;
    let id = OperationId {
        client_id: uuid::Uuid::new_v4(),
        sequence: 1,
    };
    let pinned = cluster.nodes[&leader].read_snapshot();
    let receipt = cluster.nodes[&leader]
        .submit(operation(id, 7), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(receipt.result, 7);
    assert_eq!(pinned.state.0, 0);
    for node in cluster.nodes.values() {
        node.wait_for_revision(receipt.revision, Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(node.read_snapshot().state.0, 7);
    }
    let duplicate = cluster.nodes[&leader]
        .submit(operation(id, 7), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(duplicate.revision, receipt.revision);
    assert_eq!(duplicate.result, 7);
    assert!(matches!(
        cluster.nodes[&leader]
            .submit(operation(id, 9), Instant::now() + Duration::from_secs(3))
            .await,
        Err(SubmitError::SessionRejected { .. })
    ));
    cluster.stop(leader).await;
    let next = cluster.leader(None).await;
    let duplicate = cluster.nodes[&next]
        .submit(operation(id, 7), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(duplicate.result, 7);
    cluster.start(leader).await;
    let next_id = OperationId { sequence: 2, ..id };
    let second = cluster.nodes[&next]
        .submit(operation(next_id, 2), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    for node in cluster.nodes.values() {
        node.wait_for_revision(second.revision, Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
    }
    for node in cluster.nodes.values() {
        node.checkpoint(Instant::now() + Duration::from_secs(3)).await.unwrap();
    }
    for id in 1..=3 {
        cluster.stop(id).await;
    }
    for id in 1..=3 {
        cluster.start(id).await;
    }
    let leader = cluster.leader(None).await;
    let duplicate = cluster.nodes[&leader]
        .submit(operation(next_id, 2), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(duplicate.result, 9);
    assert!(matches!(cluster.nodes[&leader].operation_status(next_id), OperationStatus::Committed(_)));
    assert_eq!(cluster.nodes[&leader].read_snapshot().state.0, 9);
    cluster.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t01_minority_cannot_commit_and_healed_cluster_converges() {
    let cluster = Cluster::new().await;
    let leader = cluster.leader(None).await;
    for id in 1..=3 {
        if id != leader {
            cluster.network.set_edge_blackholed(id, leader, true).await;
        }
    }
    let id = OperationId {
        client_id: uuid::Uuid::new_v4(),
        sequence: 1,
    };
    let outcome = cluster.nodes[&leader]
        .submit(operation(id, 100), Instant::now() + Duration::from_millis(500))
        .await;
    assert!(matches!(outcome, Err(SubmitError::OutcomeUnknown { .. }) | Err(SubmitError::NotAdmitted { .. })));
    let majority = cluster.leader(Some(leader)).await;
    let id = OperationId {
        client_id: uuid::Uuid::new_v4(),
        sequence: 1,
    };
    let receipt = cluster.nodes[&majority]
        .submit(operation(id, 3), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(receipt.result, 3);
    for id in 1..=3 {
        if id != leader {
            cluster.network.set_edge_blackholed(id, leader, false).await;
        }
    }
    for node in cluster.nodes.values() {
        node.wait_for_revision(receipt.revision, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(node.read_snapshot().state.0, 3);
    }
    cluster.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t09_explicit_replacement_and_no_automatic_bootstrap() {
    let mut cluster = Cluster::with_initial(900).await;
    let leader = cluster.leader(None).await;
    assert!(
        cluster.nodes[&leader]
            .bootstrap(BTreeMap::from([(1, 1), (2, 2), (3, 3)]))
            .await
            .is_err()
    );
    cluster.start(4).await;
    assert!(!cluster.nodes[&4].status().ready);
    assert_eq!(cluster.nodes[&4].status().leader, None);
    let old = (1..=3).find(|id| *id != leader).unwrap();
    cluster.nodes[&leader]
        .replace_voter(old, 4, 4, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let voters = cluster.nodes[&leader].status().voters;
    assert_eq!(voters.len(), 3);
    assert!(!voters.contains(&old));
    assert!(voters.contains(&4));
    let receipt = cluster.nodes[&leader]
        .submit(
            operation(
                OperationId {
                    client_id: uuid::Uuid::new_v4(),
                    sequence: 1,
                },
                7,
            ),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    cluster.nodes[&4]
        .wait_for_revision(receipt.revision, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(cluster.nodes[&4].read_snapshot().state.0, 907);
    cluster.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_replicas_install_verified_base_and_follow_committed_suffix() {
    use sharedstate::ReadReplica;
    let cluster = Cluster::new().await;
    let leader = cluster.leader(None).await;
    let client_id = uuid::Uuid::new_v4();
    let first = cluster.nodes[&leader]
        .submit(operation(OperationId { client_id, sequence: 1 }, 7), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    cluster.nodes[&leader]
        .checkpoint(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    let mut replicas = Vec::new();
    for id in 10..60 {
        let io = cluster.network.start_io(id).await;
        let replica = ReadReplica::open(
            NodeConfig::new(cluster.directory.path().join(format!("reader-{id}.db")), cluster.cluster, id),
            io,
            Counter::default(),
            BTreeMap::from([(leader, leader)]),
        )
        .await
        .unwrap();
        replicas.push(replica);
    }
    for replica in &replicas {
        replica
            .wait_for_revision(first.revision, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(replica.read_snapshot().state.0, 7);
    }
    let retained = replicas[0].read_snapshot();
    let second = cluster.nodes[&leader]
        .submit(operation(OperationId { client_id, sequence: 2 }, 2), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    for replica in &replicas {
        replica
            .wait_for_revision(second.revision, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(replica.read_snapshot().state.0, 9);
    }
    assert_eq!(retained.state.0, 7);
    for replica in replicas {
        replica.shutdown(Instant::now() + Duration::from_secs(3)).await.unwrap();
    }
    cluster.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t08_retired_sessions_survive_checkpoint_and_reject_reuse() {
    let cluster = Cluster::new().await;
    let leader = cluster.leader(None).await;
    let client_id = uuid::Uuid::new_v4();
    let id = OperationId { client_id, sequence: 1 };
    cluster.nodes[&leader]
        .submit(operation(id, 2), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    let retire = OperationId { client_id, sequence: 2 };
    let receipt = cluster.nodes[&leader]
        .retire_session(retire, Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    let duplicate = cluster.nodes[&leader]
        .retire_session(retire, Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(duplicate.revision, receipt.revision);
    cluster.nodes[&leader]
        .checkpoint(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert!(matches!(cluster.nodes[&leader].operation_status(id), OperationStatus::Retired));
    assert!(matches!(
        cluster.nodes[&leader]
            .submit(operation(OperationId { client_id, sequence: 3 }, 100), Instant::now() + Duration::from_secs(3))
            .await,
        Err(SubmitError::SessionRejected { .. })
    ));
    cluster.nodes[&leader]
        .wait_for_revision(receipt.revision, Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(cluster.nodes[&leader].read_snapshot().state.0, 2);
    cluster.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t07_cancel_after_commit_before_forwarded_response_then_retry_same_id() {
    let cluster = Cluster::new().await;
    let leader = cluster.leader(None).await;
    let follower = (1..=3).find(|id| *id != leader).unwrap();
    cluster
        .network
        .set_edge_latency(leader, follower, Some(Duration::from_millis(20)))
        .await;
    let id = OperationId {
        client_id: uuid::Uuid::new_v4(),
        sequence: 1,
    };
    {
        let pending = cluster.nodes[&follower].submit(operation(id, 9), Instant::now() + Duration::from_secs(3));
        tokio::pin!(pending);
        tokio::select! {
            result = &mut pending => panic!("response arrived before cancellation point: {result:?}"),
            _ = tokio::time::timeout(Duration::from_secs(3), async {
                while !matches!(cluster.nodes[&leader].operation_status(id), OperationStatus::Committed(_)) { tokio::time::sleep(Duration::from_millis(1)).await; }
            }) => {}
        }
    }
    assert!(matches!(cluster.nodes[&leader].operation_status(id), OperationStatus::Committed(_)));
    cluster.network.set_edge_latency(leader, follower, None).await;
    let receipt = cluster.nodes[&leader]
        .submit(operation(id, 9), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(receipt.result, 9);
    cluster.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn five_voters_keep_majority_after_two_losses() {
    let mut cluster = Cluster {
        directory: tempfile::tempdir().unwrap(),
        cluster: uuid::Uuid::new_v4(),
        network: SimulatedNet::new(),
        nodes: BTreeMap::new(),
    };
    for id in 1..=5 {
        cluster.start(id).await;
    }
    cluster.nodes[&1]
        .bootstrap((1..=5).map(|id| (id, id)).collect())
        .await
        .unwrap();
    let leader = cluster.leader(None).await;
    let other = (1..=5).find(|id| *id != leader).unwrap();
    cluster.stop(leader).await;
    cluster.stop(other).await;
    let next = cluster.leader(None).await;
    let receipt = cluster.nodes[&next]
        .submit(
            operation(
                OperationId {
                    client_id: uuid::Uuid::new_v4(),
                    sequence: 1,
                },
                5,
            ),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(receipt.result, 5);
    for node in cluster.nodes.values() {
        node.wait_for_revision(receipt.revision, Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
    }
    cluster.close().await;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct DivergentCounter(u64);
impl ReplicatedState for DivergentCounter {
    type Command = u64;
    type Result = u64;
    const SCHEMA_VERSION: u32 = 1;
    fn apply(&mut self, amount: u64) -> u64 {
        self.0 += amount + 1;
        self.0
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t21_checkpoint_mismatch_recovers_from_verified_snapshot() {
    use sharedstate::ReadReplica;
    let cluster = Cluster::new().await;
    let leader = cluster.leader(None).await;
    let client_id = uuid::Uuid::new_v4();
    let first = cluster.nodes[&leader]
        .submit(operation(OperationId { client_id, sequence: 1 }, 7), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    cluster.nodes[&leader]
        .checkpoint(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    let replica = ReadReplica::open(
        NodeConfig::new(cluster.directory.path().join("divergent.db"), cluster.cluster, 10),
        cluster.network.start_io(10).await,
        DivergentCounter::default(),
        BTreeMap::from([(leader, leader)]),
    )
    .await
    .unwrap();
    replica
        .wait_for_revision(first.revision, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let second = cluster.nodes[&leader]
        .submit(operation(OperationId { client_id, sequence: 2 }, 2), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    replica
        .wait_for_revision(second.revision, Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(replica.read_snapshot().state.0, 10);
    cluster.nodes[&leader]
        .checkpoint(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while replica.verification_failures() == 0 || replica.read_snapshot().state.0 != 9 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    replica.shutdown(Instant::now() + Duration::from_secs(3)).await.unwrap();
    cluster.close().await;
}

#[tokio::test]
async fn node_identity_is_persisted_independently_from_addresses() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("raft.db");
    let cluster = uuid::Uuid::new_v4();
    let first = NodeConfig::persistent(path.clone(), cluster).await.unwrap();
    let second = NodeConfig::persistent(path.clone(), cluster).await.unwrap();
    assert_eq!(first.node_id, second.node_id);
    assert!(NodeConfig::persistent(path, uuid::Uuid::new_v4()).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t09_replace_every_voter_preserves_imported_state_and_receipts() {
    let mut cluster = Cluster::with_initial(900).await;
    let client_id = uuid::Uuid::new_v4();
    for old in 1..=3 {
        let new = old + 3;
        cluster.start(new).await;
        let leader = cluster.leader(None).await;
        cluster.nodes[&leader]
            .replace_voter(old, new, new, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();
        cluster.stop(old).await;
        let leader = cluster.leader(None).await;
        let receipt = cluster.nodes[&leader]
            .submit(
                operation(
                    OperationId {
                        client_id,
                        sequence: old,
                    },
                    1,
                ),
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert_eq!(receipt.result, 900 + old);
        cluster.nodes[&new]
            .wait_for_revision(receipt.revision, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(cluster.nodes[&new].read_snapshot().state.0, 900 + old);
    }
    let leader = cluster.leader(None).await;
    assert_eq!(cluster.nodes[&leader].status().voters, [4, 5, 6].into_iter().collect());
    let duplicate = cluster.nodes[&leader]
        .submit(operation(OperationId { client_id, sequence: 3 }, 1), Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(duplicate.result, 903);
    cluster.close().await;
}
