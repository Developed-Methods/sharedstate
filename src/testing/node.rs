use crate::{
    cluster::{
        election::{valid_local_leader_path, valid_remote_leader_path},
        node::{recv_timeout, send_expect_ok, PROTOCOL_VERSION},
        NodeActionSender, NodeState, NodeTiming, SendActionError,
    },
    protocol::messages::{SharePeerDetails, SyncRequest, SyncResponse},
    state::{determinstic_state::DeterministicState, recoverable_state::RecoverableState},
    transport::{
        channels::NetIoSettings,
        simulated::{SimulatedIo, SimulatedNet},
        traits::{SyncConnection, SyncIO, SyncIOListener},
    },
    utils::now_ms,
};
use message_encoding::MessageEncoding;
use std::{collections::BTreeMap, future::Future, num::NonZeroU64, sync::Arc, time::Duration};
use tokio::task::JoinHandle;

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestState {
    seq: u64,
    values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TestAction {
    Set { key: String, value: String },
}

impl DeterministicState for TestState {
    type Action = TestAction;
    type AuthorityAction = TestAction;

    fn accept_seq(&self) -> u64 {
        self.seq
    }

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
        action
    }

    fn update(&mut self, action: &Self::AuthorityAction) {
        match action {
            TestAction::Set { key, value } => {
                self.values.insert(key.clone(), value.clone());
            }
        }
        self.seq += 1;
    }
}

impl MessageEncoding for TestAction {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += match self {
            Self::Set { key, value } => {
                sum += 1u16.write_to(out)?;
                sum += key.write_to(out)?;
                value.write_to(out)?
            }
        };
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        match u16::read_from(read)? {
            1 => Ok(Self::Set {
                key: MessageEncoding::read_from(read)?,
                value: MessageEncoding::read_from(read)?,
            }),
            id => Err(crate::utils::unknown_id_err(id, "TestAction")),
        }
    }
}

impl MessageEncoding for TestState {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += self.seq.write_to(out)?;
        sum += (self.values.len() as u64).write_to(out)?;
        for (key, value) in &self.values {
            sum += key.write_to(out)?;
            sum += value.write_to(out)?;
        }
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        let seq = MessageEncoding::read_from(read)?;
        let len = u64::read_from(read)? as usize;
        let mut values = BTreeMap::new();
        for _ in 0..len {
            values.insert(MessageEncoding::read_from(read)?, MessageEncoding::read_from(read)?);
        }
        Ok(Self { seq, values })
    }
}

async fn node(address: u64, can_lead: bool) -> NodeState<u64, TestState> {
    node_with_timing(address, can_lead, NodeTiming::default()).await
}

async fn node_with_timing(address: u64, can_lead: bool, timing: NodeTiming) -> NodeState<u64, TestState> {
    NodeState::new_with_timing(
        address,
        RecoverableState::new(
            address,
            TestState {
                seq: 1,
                values: BTreeMap::new(),
            },
        ),
        can_lead,
        NetIoSettings::default(),
        timing,
    )
    .await
}

struct TestClusterNode {
    address: u64,
    node: NodeState<u64, TestState>,
    listener: JoinHandle<()>,
    client: JoinHandle<()>,
    actions: NodeActionSender<TestAction, u64>,
}

#[derive(Clone)]
struct MisreportingIo {
    inner: Arc<SimulatedIo>,
    reported_remote: u64,
}

impl SyncIO for MisreportingIo {
    type Address = u64;
    type Read = <SimulatedIo as SyncIO>::Read;
    type Write = <SimulatedIo as SyncIO>::Write;

    async fn connect(&self, remote: &Self::Address) -> std::io::Result<SyncConnection<Self>> {
        let conn = self.inner.connect(remote).await?;
        Ok(SyncConnection {
            remote: self.reported_remote,
            read: conn.read,
            write: conn.write,
        })
    }
}

impl SyncIOListener for MisreportingIo {
    async fn next_client(&self) -> std::io::Result<SyncConnection<Self>> {
        let conn = self.inner.next_client().await?;
        Ok(SyncConnection {
            remote: self.reported_remote,
            read: conn.read,
            write: conn.write,
        })
    }
}

impl TestClusterNode {
    async fn stop(self, net: &SimulatedNet) {
        self.listener.abort();
        self.client.abort();
        net.stop_node(self.address).await;
    }
}

async fn start_cluster_node(net: &SimulatedNet, address: u64, can_lead: bool, peers: &[u64]) -> TestClusterNode {
    start_cluster_node_with_timing(net, address, can_lead, peers, NodeTiming::default(), NetIoSettings::default()).await
}

async fn start_cluster_node_with_timing(
    net: &SimulatedNet,
    address: u64,
    can_lead: bool,
    peers: &[u64],
    timing: NodeTiming,
    io_settings: NetIoSettings,
) -> TestClusterNode {
    let io = net.start_io(address).await;
    let node = NodeState::new_with_timing(
        address,
        RecoverableState::new(
            address,
            TestState {
                seq: 1,
                values: BTreeMap::new(),
            },
        ),
        can_lead,
        io_settings,
        timing,
    )
    .await;
    node.discover_peers(peers.iter().copied()).await;
    let listener = node.start_listener(io.clone()).await;
    let client = node.start_client(io.clone()).await;
    let actions = node.action_sender();
    TestClusterNode {
        address,
        node,
        listener,
        client,
        actions,
    }
}

async fn wait_until<F, Fut>(timeout: Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check().await {
            return true;
        }
        if deadline <= tokio::time::Instant::now() {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_leader(node: &NodeState<u64, TestState>, expected: Option<u64>, timeout: Duration) -> bool {
    wait_until(timeout, || async { node.debug_info().await.leader == expected }).await
}

async fn wait_for_leader_path(node: &NodeState<u64, TestState>, expected: Vec<u64>, timeout: Duration) -> bool {
    wait_until(timeout, || {
        let expected = expected.clone();
        async move { node.debug_info().await.leader_path == Some(expected) }
    })
    .await
}

#[tokio::test]
async fn send_returns_no_leader_when_no_leader() {
    let node = node(9001, false).await;
    let actions = node.action_sender();

    let result = actions
        .send(TestAction::Set {
            key: "leaderless".to_owned(),
            value: "rejected".to_owned(),
        })
        .await;

    assert!(matches!(result, Err(SendActionError::NoLeader)));
}

#[tokio::test]
async fn send_when_leader_unblocks_after_election() {
    let net = SimulatedNet::new();
    let node = start_cluster_node(&net, 9002, true, &[]).await;
    let mut actions = node.actions.clone();

    let result = tokio::time::timeout(
        Duration::from_secs(3),
        actions.send_when_leader(TestAction::Set {
            key: "after-election".to_owned(),
            value: "accepted".to_owned(),
        }),
    )
    .await;

    assert!(matches!(result, Ok(Ok(()))));
    assert!(
        wait_until(Duration::from_secs(3), || async {
            let mut handle = node.node.create_state_handle();
            let value = handle.read().state().values.get("after-election").cloned();
            handle.quiescent();
            value.as_deref() == Some("accepted")
        })
        .await
    );

    node.stop(&net).await;
}

#[path = "node/fuzzy.rs"]
mod fuzzy;

#[test]
fn remote_leader_path_rejects_cycles_and_local_node() {
    assert!(valid_remote_leader_path(Some(1), &[1, 2], 2, 3));
    assert!(valid_remote_leader_path(Some(1), &[1], 1, 3));
    assert!(!valid_remote_leader_path(Some(1), &[1, 2], 4, 3));
    assert!(!valid_remote_leader_path(Some(1), &[1, 2, 3], 3, 3));
    assert!(!valid_remote_leader_path(Some(1), &[1, 2, 2], 2, 3));
    assert!(!valid_remote_leader_path(Some(2), &[1, 2], 2, 3));
}

#[test]
fn local_leader_path_must_end_at_local_node() {
    assert!(valid_local_leader_path(Some(1), &[1, 2, 3], 3));
    assert!(!valid_local_leader_path(Some(1), &[1, 2], 3));
    assert!(!valid_local_leader_path(Some(1), &[1, 2, 2], 2));
}

#[tokio::test]
async fn can_lead_node_promotes_when_other_can_lead_peers_are_inactive() {
    let node = node(2, true).await;

    node.inner
        .discover_peers(
            [
                SharePeerDetails {
                    address: 1,
                    can_be_leader: Some(true),
                    last_global_activity: None,
                },
                SharePeerDetails {
                    address: 3,
                    can_be_leader: Some(true),
                    last_global_activity: None,
                },
            ]
            .into_iter(),
        )
        .await;

    node.inner.apply_election().await;

    let control = node.inner.control.lock().await;
    assert_eq!(control.leader.leader, Some(2));
    assert_eq!(control.leader.path, Some(vec![2]));
}

#[tokio::test]
async fn can_lead_node_promotes_with_majority_reachability() {
    let node = node(2, true).await;

    node.inner
        .discover_peers(
            [
                SharePeerDetails {
                    address: 1,
                    can_be_leader: Some(true),
                    last_global_activity: None,
                },
                SharePeerDetails {
                    address: 3,
                    can_be_leader: Some(true),
                    last_global_activity: None,
                },
            ]
            .into_iter(),
        )
        .await;

    {
        let mut control = node.inner.control.lock().await;
        let peer = control.peers.get_mut(&3).unwrap().as_mut().unwrap();
        peer.last_activity = NonZeroU64::new(now_ms());
        peer.connected = true;
    }

    node.inner.apply_election().await;

    let control = node.inner.control.lock().await;
    assert_eq!(control.leader.leader, Some(2));
    assert_eq!(control.leader.path, Some(vec![2]));
    assert!(0 < control.leader.term);
}

#[tokio::test]
async fn local_observation_does_not_advertise_remote_leader_without_follow_path() {
    let node = node(2, true).await;

    {
        let mut control = node.inner.control.lock().await;
        control.leader.leader = Some(1);
        control.leader.path = Some(vec![1]);
        control.leader.term = 4;
    }

    let observation = node.inner.local_observation().await;
    assert_eq!(observation.leader, None);
    assert_eq!(observation.leader_path, None);
    assert_eq!(observation.term, 4);
}

#[tokio::test]
async fn leader_rejoin_then_second_failover_promotes_available_candidate() {
    let net = SimulatedNet::new();

    let node7001 = start_cluster_node(&net, 7001, true, &[7002, 7003]).await;
    let mut node7002 = Some(start_cluster_node(&net, 7002, true, &[7001, 7003]).await);
    let node7003 = start_cluster_node(&net, 7003, false, &[7001, 7002]).await;

    assert!(wait_for_leader(&node7001.node, Some(7001), Duration::from_secs(3)).await);
    assert!(
        wait_until(Duration::from_secs(3), || async {
            let leader = node7002.as_ref().unwrap().node.debug_info().await.leader;
            leader == Some(7001)
        })
        .await
    );
    assert!(wait_for_leader(&node7003.node, Some(7001), Duration::from_secs(3)).await);

    node7001.stop(&net).await;

    assert!(
        wait_until(Duration::from_secs(3), || async {
            let leader = node7002.as_ref().unwrap().node.debug_info().await.leader;
            leader == Some(7002)
        })
        .await
    );
    assert!(wait_for_leader(&node7003.node, Some(7002), Duration::from_secs(3)).await);

    node7002.take().unwrap().stop(&net).await;
    assert!(wait_for_leader(&node7003.node, None, Duration::from_secs(3)).await);

    let node7001 = start_cluster_node(&net, 7001, true, &[7002, 7003]).await;
    assert!(wait_for_leader(&node7001.node, Some(7001), Duration::from_secs(3)).await);
    assert!(wait_for_leader(&node7003.node, Some(7001), Duration::from_secs(3)).await);

    let node7002_restarted = start_cluster_node(&net, 7002, true, &[7001, 7003]).await;
    assert!(wait_for_leader(&node7002_restarted.node, Some(7001), Duration::from_secs(3)).await);

    node7001.stop(&net).await;

    if !wait_for_leader(&node7002_restarted.node, Some(7002), Duration::from_secs(3)).await {
        panic!("{:#?}", node7002_restarted.node.debug_info().await);
    }
    let debug = node7002_restarted.node.debug_info().await;
    assert_eq!(debug.leader_path, Some(vec![7002]));
    assert_eq!(debug.follow_remote, None);

    node7002_restarted
        .actions
        .send(TestAction::Set {
            key: "after_failover".to_owned(),
            value: "ok".to_owned(),
        })
        .await
        .unwrap();

    assert!(
        wait_until(Duration::from_secs(3), || async {
            let mut handle = node7002_restarted.node.create_state_handle();
            let value = handle.read().state().values.get("after_failover").cloned();
            handle.quiescent();
            value.as_deref() == Some("ok")
        })
        .await
    );

    node7002_restarted.stop(&net).await;
    node7003.stop(&net).await;
}

#[tokio::test]
async fn leader_path_update_propagates_to_downstream_followers() {
    let net = SimulatedNet::new();

    let node7001 = start_cluster_node(&net, 7001, true, &[7002, 7003, 7004]).await;
    let node7002 = start_cluster_node(&net, 7002, false, &[7001, 7003, 7004]).await;
    let node7003 = start_cluster_node(&net, 7003, false, &[7001, 7002, 7004]).await;
    let node7004 = start_cluster_node(&net, 7004, false, &[7001, 7002, 7003]).await;

    assert!(wait_for_leader(&node7001.node, Some(7001), Duration::from_secs(3)).await);
    assert!(wait_for_leader(&node7002.node, Some(7001), Duration::from_secs(3)).await);
    assert!(wait_for_leader(&node7003.node, Some(7001), Duration::from_secs(3)).await);
    assert!(wait_for_leader(&node7004.node, Some(7001), Duration::from_secs(3)).await);
    assert!(wait_for_leader_path(&node7002.node, vec![7001, 7002], Duration::from_secs(3)).await);
    assert!(wait_for_leader_path(&node7003.node, vec![7001, 7003], Duration::from_secs(3)).await);
    assert!(wait_for_leader_path(&node7004.node, vec![7001, 7004], Duration::from_secs(3)).await);

    net.set_edge_blocked(7002, 7001, true).await;

    assert!(
        wait_until(Duration::from_secs(3), || async {
            let debug = node7002.node.debug_info().await;
            debug.leader == Some(7001)
                && debug
                    .follow_remote
                    .is_some_and(|remote| remote == 7003 || remote == 7004)
                && debug.follow_remote != Some(7001)
                && debug
                    .follow_leader_path
                    .as_ref()
                    .is_some_and(|path| path.first() == Some(&7001) && path.last() == Some(&7002))
        })
        .await,
        "{:#?}",
        node7002.node.debug_info().await
    );

    let relay = node7002.node.debug_info().await.follow_remote.unwrap();
    let backup = if relay == 7003 { 7004 } else { 7003 };

    net.set_edge_blocked(7002, backup, true).await;
    net.set_edge_blocked(relay, 7001, true).await;

    let relay_node = if relay == 7003 { &node7003.node } else { &node7004.node };

    let expected_relay_path = vec![7001, backup, relay];
    let expected_node_path = vec![7001, backup, relay, 7002];

    assert!(
        wait_for_leader_path(relay_node, expected_relay_path.clone(), Duration::from_secs(3)).await,
        "relay={relay} backup={backup} relay_debug={:#?}",
        relay_node.debug_info().await
    );

    assert!(
        wait_until(Duration::from_secs(3), || {
            let expected_node_path = expected_node_path.clone();
            async {
                let debug = node7002.node.debug_info().await;
                debug.leader == Some(7001)
                    && debug.leader_path == Some(expected_node_path.clone())
                    && debug.follow_remote == Some(relay)
                    && debug.follow_leader_path == Some(expected_node_path)
            }
        })
        .await,
        "relay={relay} backup={backup} node7002_debug={:#?}",
        node7002.node.debug_info().await
    );

    node7001.stop(&net).await;
    node7002.stop(&net).await;
    node7003.stop(&net).await;
    node7004.stop(&net).await;
}

#[tokio::test]
async fn non_leader_does_not_periodically_observe_known_non_leader_peers() {
    let net = SimulatedNet::new();

    let node1 = start_cluster_node(&net, 1, true, &[2, 3, 4]).await;
    let node2 = start_cluster_node(&net, 2, false, &[1, 3, 4]).await;
    let node3 = start_cluster_node(&net, 3, false, &[1, 2, 4]).await;
    let node4 = start_cluster_node(&net, 4, false, &[1, 2, 3]).await;

    assert!(wait_for_leader(&node1.node, Some(1), Duration::from_secs(3)).await);
    assert!(wait_for_leader(&node2.node, Some(1), Duration::from_secs(3)).await);
    assert!(wait_for_leader(&node3.node, Some(1), Duration::from_secs(3)).await);
    assert!(wait_for_leader(&node4.node, Some(1), Duration::from_secs(3)).await);

    tokio::time::sleep(NodeTiming::default().observation_interval * 2).await;

    let debug = node2.node.debug_info().await;
    let peer1 = debug.peers.iter().find(|peer| peer.address == 1).unwrap();
    let peer3 = debug.peers.iter().find(|peer| peer.address == 3).unwrap();
    let peer4 = debug.peers.iter().find(|peer| peer.address == 4).unwrap();

    assert_eq!(peer1.observed_leader, Some(1), "{debug:#?}");
    assert_eq!(peer3.observed_leader, None, "{debug:#?}");
    assert_eq!(peer4.observed_leader, None, "{debug:#?}");
    assert_eq!(peer1.connected, Some(true), "{debug:#?}");
    assert_eq!(peer3.connected, Some(false), "{debug:#?}");
    assert_eq!(peer4.connected, Some(false), "{debug:#?}");

    node1.stop(&net).await;
    node2.stop(&net).await;
    node3.stop(&net).await;
    node4.stop(&net).await;
}

#[tokio::test]
async fn short_lived_probe_close_does_not_clear_active_follow_connection() {
    let net = SimulatedNet::new();

    let node1 = start_cluster_node(&net, 1, true, &[2]).await;
    let node2 = start_cluster_node(&net, 2, false, &[1]).await;

    assert!(
        wait_until(Duration::from_secs(3), || async {
            let debug1 = node1.node.debug_info().await;
            let debug2 = node2.node.debug_info().await;
            let peer2 = debug1.peers.iter().find(|peer| peer.address == 2);
            debug1.leader == Some(1)
                && debug2.leader == Some(1)
                && debug2.follow_remote == Some(1)
                && peer2.is_some_and(|peer| peer.connected == Some(true))
        })
        .await,
        "node1={:#?}\nnode2={:#?}",
        node1.node.debug_info().await,
        node2.node.debug_info().await
    );

    let probe_io = net.start_io(9000).await;
    let conn = probe_io.connect(&1).await.unwrap();
    let (_remote, write, mut read) = conn.client_channels::<TestState>(NetIoSettings::default());

    assert!(
        send_expect_ok(&write, &mut read, SyncRequest::ProtocolVersion(PROTOCOL_VERSION), Duration::from_secs(1),)
            .await
    );
    assert!(send_expect_ok(&write, &mut read, SyncRequest::MyAddress(2), Duration::from_secs(1),).await);
    assert!(write.send(SyncRequest::Ping(42)).await.is_ok());
    assert!(matches!(recv_timeout(&mut read, Duration::from_secs(1)).await, Some(SyncResponse::Pong(42))));
    drop(write);
    drop(read);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let debug1 = node1.node.debug_info().await;
    let peer2 = debug1.peers.iter().find(|peer| peer.address == 2).unwrap();
    assert_eq!(peer2.connected, Some(true), "{debug1:#?}");

    node1.stop(&net).await;
    node2.stop(&net).await;
    net.stop_node(9000).await;
}

#[tokio::test]
async fn non_leader_falls_back_to_non_leader_relay_when_can_lead_peers_are_unreachable() {
    let net = SimulatedNet::new();

    let node1 = start_cluster_node(&net, 1, true, &[2, 3, 4]).await;
    let node2 = start_cluster_node(&net, 2, true, &[1, 3, 4]).await;
    let node3 = start_cluster_node(&net, 3, false, &[1, 2, 4]).await;
    let node4 = start_cluster_node(&net, 4, false, &[1, 2]).await;

    assert!(wait_for_leader(&node1.node, Some(1), Duration::from_secs(3)).await);
    assert!(wait_for_leader(&node2.node, Some(1), Duration::from_secs(3)).await);
    assert!(wait_for_leader(&node3.node, Some(1), Duration::from_secs(3)).await);
    assert!(wait_for_leader(&node4.node, Some(1), Duration::from_secs(3)).await);

    net.set_edge_blocked(4, 1, true).await;
    net.set_edge_blocked(4, 2, true).await;

    assert!(
        wait_until(Duration::from_secs(5), || async {
            let debug = node4.node.debug_info().await;
            debug.leader == Some(1)
                && debug.follow_remote == Some(3)
                && debug.leader_path == Some(vec![1, 3, 4])
                && debug.follow_leader_path == Some(vec![1, 3, 4])
        })
        .await,
        "node3={:#?}\nnode4={:#?}",
        node3.node.debug_info().await,
        node4.node.debug_info().await
    );

    node1.stop(&net).await;
    node2.stop(&net).await;
    node3.stop(&net).await;
    node4.stop(&net).await;
}

#[tokio::test]
async fn node_identity_comes_from_my_address_not_transport_address() {
    let net = SimulatedNet::new();
    let io1 = Arc::new(MisreportingIo {
        inner: net.start_io(1).await,
        reported_remote: 9001,
    });
    let io2 = Arc::new(MisreportingIo {
        inner: net.start_io(2).await,
        reported_remote: 9002,
    });

    let node1 = node(1, true).await;
    let node2 = node(2, false).await;
    node1.discover_peers([2].into_iter()).await;
    node2.discover_peers([1].into_iter()).await;

    let listener1 = node1.start_listener(io1.clone()).await;
    let client1 = node1.start_client(io1).await;
    let listener2 = node2.start_listener(io2.clone()).await;
    let client2 = node2.start_client(io2).await;

    assert!(
        wait_until(Duration::from_secs(3), || async {
            let debug1 = node1.debug_info().await;
            let debug2 = node2.debug_info().await;
            let peer2 = debug1.peers.iter().find(|peer| peer.address == 2);
            debug1.leader == Some(1)
                && debug2.leader == Some(1)
                && debug2.follow_remote == Some(1)
                && debug2
                    .peers
                    .iter()
                    .all(|peer| peer.address != 9001 && peer.address != 9002)
                && peer2.is_some_and(|peer| peer.connected == Some(true))
        })
        .await,
        "node1={:#?}\nnode2={:#?}",
        node1.debug_info().await,
        node2.debug_info().await
    );

    listener1.abort();
    client1.abort();
    listener2.abort();
    client2.abort();
    net.stop_node(1).await;
    net.stop_node(2).await;
}

#[tokio::test]
async fn high_latency_cluster_converges_with_configured_timing() {
    let net = SimulatedNet::new();
    for (a, b) in [(1, 2), (1, 3), (2, 3)] {
        net.set_edge_latency(a, b, Some(Duration::from_millis(500))).await;
    }

    let timing = NodeTiming {
        observation_stale_after: Duration::from_secs(30),
        observation_interval: Duration::from_millis(250),
        follow_retry_interval: Duration::from_millis(250),
        rpc_timeout: Duration::from_secs(10),
    };
    let io_settings = NetIoSettings {
        process_timeout: Duration::from_secs(5),
        message_timeout: Duration::from_secs(20),
    };

    let node1 = start_cluster_node_with_timing(&net, 1, true, &[2, 3], timing.clone(), io_settings.clone()).await;
    let node2 = start_cluster_node_with_timing(&net, 2, true, &[1, 3], timing.clone(), io_settings.clone()).await;
    let node3 = start_cluster_node_with_timing(&net, 3, false, &[1, 2], timing, io_settings).await;

    assert!(
        wait_until(Duration::from_secs(25), || async {
            let debug1 = node1.node.debug_info().await;
            let debug2 = node2.node.debug_info().await;
            let debug3 = node3.node.debug_info().await;
            let Some(leader) = debug1.leader else {
                return false;
            };
            debug2.leader == Some(leader) && debug3.leader == Some(leader)
        })
        .await,
        "node1={:#?}\nnode2={:#?}\nnode3={:#?}",
        node1.node.debug_info().await,
        node2.node.debug_info().await,
        node3.node.debug_info().await
    );

    let mut actions = node3.actions.clone();
    actions
        .send_when_leader(TestAction::Set {
            key: "slow".to_owned(),
            value: "ok".to_owned(),
        })
        .await
        .unwrap();

    assert!(
        wait_until(Duration::from_secs(20), || async {
            [&node1.node, &node2.node, &node3.node].into_iter().all(|node| {
                let mut handle = node.create_state_handle();
                let value = handle.read().state().values.get("slow").cloned();
                handle.quiescent();
                value.as_deref() == Some("ok")
            })
        })
        .await,
        "node1={:#?}\nnode2={:#?}\nnode3={:#?}",
        node1.node.debug_info().await,
        node2.node.debug_info().await,
        node3.node.debug_info().await
    );

    node1.stop(&net).await;
    node2.stop(&net).await;
    node3.stop(&net).await;
}

#[tokio::test]
async fn promoted_leader_uses_higher_term_than_isolated_old_leader() {
    let net = SimulatedNet::new();

    let node1 = start_cluster_node(&net, 1, true, &[2, 3]).await;
    let node2 = start_cluster_node(&net, 2, true, &[1, 3]).await;
    let node3 = start_cluster_node(&net, 3, false, &[1, 2]).await;

    assert!(wait_for_leader(&node1.node, Some(1), Duration::from_secs(3)).await);
    assert!(wait_for_leader(&node2.node, Some(1), Duration::from_secs(3)).await);
    assert!(wait_for_leader(&node3.node, Some(1), Duration::from_secs(3)).await);
    let node1_term_before_isolation = node1.node.debug_info().await.term;

    net.set_node_blocked(1, true).await;

    assert!(wait_for_leader(&node2.node, Some(2), Duration::from_secs(3)).await);
    assert!(node2.node.debug_info().await.term > node1_term_before_isolation);

    node2
        .actions
        .send(TestAction::Set {
            key: "after_promotion".to_owned(),
            value: "ok".to_owned(),
        })
        .await
        .unwrap();

    assert!(
        wait_until(Duration::from_secs(3), || async {
            let mut handle = node2.node.create_state_handle();
            let value = handle.read().state().values.get("after_promotion").cloned();
            handle.quiescent();
            value.as_deref() == Some("ok")
        })
        .await
    );

    net.set_node_blocked(1, false).await;

    assert!(
        wait_until(Duration::from_secs(5), || async {
            let debug1 = node1.node.debug_info().await;
            let debug2 = node2.node.debug_info().await;
            let debug3 = node3.node.debug_info().await;
            debug1.leader == Some(2)
                && debug2.leader == Some(2)
                && debug3.leader == Some(2)
                && debug1.follow_remote.is_some()
                && debug2.follow_remote.is_none()
        })
        .await,
        "node1={:#?}\nnode2={:#?}\nnode3={:#?}",
        node1.node.debug_info().await,
        node2.node.debug_info().await,
        node3.node.debug_info().await
    );

    node1.stop(&net).await;
    node2.stop(&net).await;
    node3.stop(&net).await;
}

#[tokio::test]
async fn higher_term_rejoining_node_does_not_steal_from_higher_agreement_leader() {
    let net = SimulatedNet::new();

    let node1 = start_cluster_node(&net, 1, true, &[2, 3]).await;
    let node2 = start_cluster_node(&net, 2, true, &[1, 3]).await;
    let node3 = start_cluster_node(&net, 3, false, &[1, 2]).await;

    assert!(wait_for_leader(&node1.node, Some(1), Duration::from_secs(3)).await);
    assert!(wait_for_leader(&node2.node, Some(1), Duration::from_secs(3)).await);
    assert!(wait_for_leader(&node3.node, Some(1), Duration::from_secs(3)).await);

    node2
        .actions
        .send(TestAction::Set {
            key: "first".to_owned(),
            value: "one".to_owned(),
        })
        .await
        .unwrap();

    assert!(
        wait_until(Duration::from_secs(3), || async {
            [&node1.node, &node2.node, &node3.node].into_iter().all(|node| {
                let mut handle = node.create_state_handle();
                let value = handle.read().state().values.get("first").cloned();
                handle.quiescent();
                value.as_deref() == Some("one")
            })
        })
        .await
    );

    net.set_node_blocked(1, true).await;

    assert!(wait_for_leader(&node2.node, Some(2), Duration::from_secs(3)).await);

    node2
        .actions
        .send(TestAction::Set {
            key: "second".to_owned(),
            value: "two".to_owned(),
        })
        .await
        .unwrap();

    assert!(
        wait_until(Duration::from_secs(3), || async {
            [&node2.node, &node3.node].into_iter().all(|node| {
                let mut handle = node.create_state_handle();
                let value = handle.read().state().values.get("second").cloned();
                handle.quiescent();
                value.as_deref() == Some("two")
            })
        })
        .await
    );

    net.set_node_blocked(1, false).await;

    assert!(
        wait_until(Duration::from_secs(5), || async {
            let debug1 = node1.node.debug_info().await;
            let debug2 = node2.node.debug_info().await;
            let debug3 = node3.node.debug_info().await;
            debug1.leader == Some(2)
                && debug2.leader == Some(2)
                && debug3.leader == Some(2)
                && debug1.follow_remote.is_some()
                && debug2.follow_remote.is_none()
        })
        .await,
        "node1={:#?}\nnode2={:#?}\nnode3={:#?}",
        node1.node.debug_info().await,
        node2.node.debug_info().await,
        node3.node.debug_info().await
    );

    net.set_node_blocked(1, true).await;

    assert!(
        wait_until(Duration::from_secs(5), || async {
            let debug1 = node1.node.debug_info().await;
            let debug2 = node2.node.debug_info().await;
            debug1.leader == Some(1) && debug1.term > debug2.term
        })
        .await,
        "node1={:#?}\nnode2={:#?}",
        node1.node.debug_info().await,
        node2.node.debug_info().await
    );

    node2
        .actions
        .send(TestAction::Set {
            key: "third".to_owned(),
            value: "three".to_owned(),
        })
        .await
        .unwrap();

    assert!(
        wait_until(Duration::from_secs(3), || async {
            [&node2.node, &node3.node].into_iter().all(|node| {
                let mut handle = node.create_state_handle();
                let value = handle.read().state().values.get("third").cloned();
                handle.quiescent();
                value.as_deref() == Some("three")
            })
        })
        .await
    );

    net.set_node_blocked(1, false).await;

    assert!(
        wait_until(Duration::from_secs(5), || async {
            let debug1 = node1.node.debug_info().await;
            let debug2 = node2.node.debug_info().await;
            let debug3 = node3.node.debug_info().await;
            if debug1.leader != Some(2)
                || debug2.leader != Some(2)
                || debug3.leader != Some(2)
                || debug1.follow_remote.is_none()
                || debug2.follow_remote.is_some()
            {
                return false;
            }

            [&node1.node, &node2.node, &node3.node].into_iter().all(|node| {
                let mut handle = node.create_state_handle();
                let state = handle.read().state().clone();
                handle.quiescent();
                state.values.get("first").is_some_and(|value| value == "one")
                    && state.values.get("second").is_some_and(|value| value == "two")
                    && state.values.get("third").is_some_and(|value| value == "three")
            })
        })
        .await,
        "node1={:#?}\nnode2={:#?}\nnode3={:#?}",
        node1.node.debug_info().await,
        node2.node.debug_info().await,
        node3.node.debug_info().await
    );

    node1.stop(&net).await;
    node2.stop(&net).await;
    node3.stop(&net).await;
}
