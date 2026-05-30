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
