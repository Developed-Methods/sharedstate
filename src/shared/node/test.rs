use super::*;
use crate::net::{message_channel::NetIoSettings, simulated::SimulatedNet};
use std::collections::BTreeMap;

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
    NodeState::new(
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
    )
    .await
}

struct TestClusterNode {
    address: u64,
    node: NodeState<u64, TestState>,
    listener: JoinHandle<()>,
    client: JoinHandle<()>,
    actions: NodeActionSender<TestAction>,
}

impl TestClusterNode {
    async fn stop(self, net: &SimulatedNet) {
        self.listener.abort();
        self.client.abort();
        net.stop_node(self.address).await;
    }
}

async fn start_cluster_node(net: &SimulatedNet, address: u64, can_lead: bool, peers: &[u64]) -> TestClusterNode {
    let io = net.start_io(address).await;
    let node = node(address, can_lead).await;
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

#[path = "test/fuzzy.rs"]
mod fuzzy;

#[test]
fn remote_leader_path_rejects_cycles_and_local_node() {
    assert!(valid_remote_leader_path(Some(1), &[1, 2], 3));
    assert!(!valid_remote_leader_path(Some(1), &[1, 2, 3], 3));
    assert!(!valid_remote_leader_path(Some(1), &[1, 2, 2], 3));
    assert!(!valid_remote_leader_path(Some(2), &[1, 2], 3));
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

    let leader = node.inner.leader.lock().await;
    assert_eq!(leader.leader, Some(2));
    assert_eq!(leader.path, Some(vec![2]));
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
        let mut peers = node.inner.peers.lock().await;
        let peer = peers.get_mut(&3).unwrap().as_mut().unwrap();
        peer.last_activity = NonZeroU64::new(now_ms());
        peer.connected = true;
    }

    node.inner.apply_election().await;

    let leader = node.inner.leader.lock().await;
    assert_eq!(leader.leader, Some(2));
    assert_eq!(leader.path, Some(vec![2]));
    assert!(0 < leader.term);
}

#[tokio::test]
async fn local_observation_does_not_advertise_remote_leader_without_follow_path() {
    let node = node(2, true).await;

    {
        let mut leader = node.inner.leader.lock().await;
        leader.leader = Some(1);
        leader.path = Some(vec![1]);
        leader.term = 4;
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
