//! High-level entry point that provisions everything a node needs to share
//! state with a cluster: the RPC listener, peer discovery, leader election,
//! and the state sync task.

use std::{collections::HashMap, sync::Arc};

use message_encoding::MessageEncoding;
use sequenced_broadcast::{SequencedBroadcastSettings, SettingsError};
use tokio::{
    sync::{
        Mutex,
        mpsc::{self, error::SendError},
    },
    task::JoinHandle,
};

use crate::{
    cluster::{
        election::EtcdElectionConfig,
        leader::EtcdLeaderTask,
        node_state::{NodeState, PeerState},
        peer_connections::PeerConnections,
        peer_discovery::{PeerDiscoveryTask, PeerDiscoveryTiming},
        rpc_server::RpcServer,
        state_sync::{StateSyncTask, StateSyncTiming},
    },
    protocol::messages::LeaderState,
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::RecoverableState,
        subscribable_state::{StateHandle, SubscribableState},
    },
    transport::{channels::NetIoSettings, traits::SyncIOListener},
    utils::unique_state_id,
};

pub struct SharedStateConfig<I: SyncIOListener, D: DeterministicState> {
    pub io: Arc<I>,
    pub my_address: I::Address,
    pub can_lead: bool,
    pub initial_peers: Vec<I::Address>,
    pub initial_state: D,
    pub settings: SharedStateSettings,
}

pub struct SharedStateRecoverableConfig<I: SyncIOListener, D: DeterministicState> {
    pub io: Arc<I>,
    pub my_address: I::Address,
    pub can_lead: bool,
    pub initial_peers: Vec<I::Address>,
    pub initial_state: RecoverableState<D>,
    pub settings: SharedStateSettings,
}

#[derive(Clone, Debug, Default)]
pub struct SharedStateSettings {
    pub net: NetIoSettings,
    pub broadcast: SequencedBroadcastSettings,
    pub discovery_timing: PeerDiscoveryTiming,
    pub election: EtcdElectionConfig,
    pub sync_timing: StateSyncTiming,
}

const ACTION_QUEUE_CAPACITY: usize = 512;

/// A running shared-state node. Dropping it stops the background tasks.
pub struct SharedState<I: SyncIOListener, D: DeterministicState> {
    node: Arc<NodeState<I::Address, D>>,
    actions_tx: mpsc::Sender<(I::Address, D::Action)>,
    tasks: Vec<JoinHandle<()>>,
}

impl<I, D> SharedState<I, D>
where
    I: SyncIOListener,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    pub fn start(config: SharedStateConfig<I, D>) -> Result<Self, SettingsError> {
        let SharedStateConfig {
            io,
            my_address,
            can_lead,
            initial_peers,
            initial_state,
            settings,
        } = config;

        let eligible = settings.election.bootstrap && can_lead;
        Self::start_inner(
            SharedStateRecoverableConfig {
                io,
                my_address,
                can_lead,
                initial_peers,
                initial_state: RecoverableState::new(unique_state_id(&my_address), initial_state),
                settings,
            },
            eligible,
        )
    }

    pub fn start_recoverable(config: SharedStateRecoverableConfig<I, D>) -> Result<Self, SettingsError> {
        Self::start_inner(config, true)
    }

    fn start_inner(config: SharedStateRecoverableConfig<I, D>, eligible: bool) -> Result<Self, SettingsError> {
        let SharedStateRecoverableConfig {
            io,
            my_address,
            can_lead,
            initial_peers,
            initial_state,
            settings,
        } = config;

        let peers = initial_peers
            .into_iter()
            .filter(|peer| *peer != my_address)
            .map(|peer| (peer, PeerState::empty(peer)))
            .collect::<HashMap<_, _>>();

        let node = Arc::new(NodeState {
            my_address,
            can_lead,
            peers: Mutex::new(peers),
            state: SubscribableState::new(initial_state, settings.broadcast.clone())?,
            election: Default::default(),
            leadership_changed: tokio::sync::Notify::new(),
            eligible: std::sync::atomic::AtomicBool::new(eligible),
            synced_epoch: Default::default(),
        });

        let peer_connections = Arc::new(PeerConnections::new(io.clone(), settings.net.clone(), node.clone()));
        let (actions_tx, actions_rx) = mpsc::channel(ACTION_QUEUE_CAPACITY);
        let rpc_server = Arc::new(RpcServer::new(node.clone(), actions_tx.clone()));

        let tasks = vec![
            rpc_server.start_listener(io.clone(), settings.net.clone()),
            tokio::spawn(
                PeerDiscoveryTask::new(node.clone(), peer_connections.clone(), settings.discovery_timing).run(),
            ),
            tokio::spawn(EtcdLeaderTask::new(node.clone(), peer_connections.clone(), settings.election).run()),
            tokio::spawn(
                StateSyncTask::new(node.clone(), peer_connections, io, settings.net, actions_rx, settings.sync_timing)
                    .run(),
            ),
        ];

        Ok(Self {
            node,
            actions_tx,
            tasks,
        })
    }

    pub fn my_address(&self) -> I::Address {
        self.node.my_address
    }

    pub fn can_lead(&self) -> bool {
        self.node.can_lead
    }

    /// The underlying node state, for inspecting peers or leader details.
    pub fn node(&self) -> &Arc<NodeState<I::Address, D>> {
        &self.node
    }

    /// A read handle over the deterministic state.
    pub fn state_handle(&self) -> StateHandle<D> {
        self.node.state.create_handle()
    }

    pub async fn leader_state(&self) -> LeaderState<I::Address> {
        self.node.current_leader()
    }

    /// Queues an action originating from this node. The sync task applies it
    /// with authority when leading and forwards it to the leader otherwise.
    pub async fn submit_action(&self, action: D::Action) -> Result<(), SendError<(I::Address, D::Action)>> {
        self.actions_tx.send((self.node.my_address, action)).await
    }

    /// Sender for queueing actions on behalf of other sources.
    pub fn actions_sender(&self) -> mpsc::Sender<(I::Address, D::Action)> {
        self.actions_tx.clone()
    }
}

impl<I: SyncIOListener, D: DeterministicState> Drop for SharedState<I, D> {
    fn drop(&mut self) {
        self.node.revoke_authority(None);
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        io::Result,
        time::{Duration, Instant},
    };

    use super::*;
    use crate::{
        cluster::{peer_discovery::PeerDiscoveryTiming, state_sync::StateSyncTiming},
        protocol::messages::LeaderMode,
        state::recoverable_state::RecoverableStateAction,
        transport::simulated::{SimulatedIo, SimulatedNet},
    };

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    struct KvState {
        seq: u64,
        values: BTreeMap<u64, u64>,
    }

    impl DeterministicState for KvState {
        type Action = (u64, u64);
        type AuthorityAction = (u64, u64);

        fn accept_seq(&self) -> u64 {
            self.seq
        }

        fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
            action
        }

        fn update(&mut self, (key, value): &Self::AuthorityAction) {
            self.values.insert(*key, *value);
            self.seq += 1;
        }
    }

    impl MessageEncoding for KvState {
        fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
            let mut sum = self.seq.write_to(out)?;
            sum += (self.values.len() as u64).write_to(out)?;
            for (key, value) in &self.values {
                sum += key.write_to(out)?;
                sum += value.write_to(out)?;
            }
            Ok(sum)
        }

        fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
            let seq = MessageEncoding::read_from(read)?;
            let len = u64::read_from(read)? as usize;
            let mut values = BTreeMap::new();
            for _ in 0..len {
                values.insert(MessageEncoding::read_from(read)?, MessageEncoding::read_from(read)?);
            }
            Ok(Self { seq, values })
        }
    }

    fn fast_settings(etcd: &crate::test_support::TestEtcd, bootstrap: bool) -> SharedStateSettings {
        SharedStateSettings {
            net: NetIoSettings {
                process_timeout: Duration::from_secs(1),
                message_timeout: Duration::from_secs(2),
            },
            broadcast: SequencedBroadcastSettings::default(),
            discovery_timing: PeerDiscoveryTiming {
                observation_interval: Duration::from_millis(50),
                max_concurrent_observations: 8,
            },
            election: etcd.config(bootstrap),
            sync_timing: StateSyncTiming {
                leader_poll_interval: Duration::from_millis(20),
                retry_delay: Duration::from_millis(50),
            },
        }
    }

    struct TestNet {
        network: SimulatedNet,
        etcd: crate::test_support::TestEtcd,
    }

    impl TestNet {
        async fn new() -> Self {
            Self {
                network: SimulatedNet::new(),
                etcd: crate::test_support::TestEtcd::start().await,
            }
        }
    }

    impl std::ops::Deref for TestNet {
        type Target = SimulatedNet;
        fn deref(&self) -> &Self::Target {
            &self.network
        }
    }

    async fn start_node(
        net: &TestNet,
        address: u64,
        can_lead: bool,
        peers: &[u64],
    ) -> SharedState<SimulatedIo, KvState> {
        let io = net.start_io(address).await;
        SharedState::start(SharedStateConfig {
            io,
            my_address: address,
            can_lead,
            initial_peers: peers.to_vec(),
            initial_state: KvState::default(),
            settings: fast_settings(&net.etcd, address == 1),
        })
        .unwrap()
    }

    async fn wait_for<F: FnMut() -> bool>(what: &str, mut check: F) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !check() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_for_value(node: &SharedState<SimulatedIo, KvState>, key: u64, value: u64) {
        let mut handle = node.state_handle();
        wait_for(&format!("node {} to see {key}={value}", node.my_address()), || {
            handle.read_with(|state| state.state().values.get(&key) == Some(&value))
        })
        .await;
    }

    async fn wait_for_state(node: &SharedState<SimulatedIo, KvState>, expected: &BTreeMap<u64, u64>) {
        let mut handle = node.state_handle();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let actual = handle.read_with(|state| state.state().clone());
            if actual.seq == expected.len() as u64 && actual.values == *expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for node {} to settle on {expected:?}, actual seq {} values {:?}",
                node.my_address(),
                actual.seq,
                actual.values,
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_for_cluster_state(nodes: &[&SharedState<SimulatedIo, KvState>], expected: &BTreeMap<u64, u64>) {
        for node in nodes {
            wait_for_state(node, expected).await;
        }
    }

    async fn wait_for_common_leader(nodes: &[&SharedState<SimulatedIo, KvState>]) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let mut observed_leader = None;
            let mut observed_epoch = None;
            let mut leader_is_leading = false;
            let mut unsettled = Vec::new();

            for node in nodes {
                let state = node.leader_state().await;
                let leader = match state.mode {
                    LeaderMode::Leading => {
                        leader_is_leading = true;
                        node.my_address()
                    }
                    LeaderMode::Following { leader } => leader,
                    _ => {
                        unsettled.push((node.my_address(), state));
                        continue;
                    }
                };

                if observed_leader.is_some_and(|observed| observed != leader)
                    || observed_epoch.is_some_and(|epoch| epoch != state.epoch)
                {
                    unsettled.push((node.my_address(), state.clone()));
                }
                observed_leader.get_or_insert(leader);
                observed_epoch.get_or_insert(state.epoch);
            }

            if let Some(leader) = observed_leader {
                if unsettled.is_empty() && leader_is_leading {
                    return leader;
                }
            }

            assert!(
                Instant::now() < deadline,
                "nodes never settled on a common leader, unsettled states {unsettled:?}",
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_for_leader(nodes: &[&SharedState<SimulatedIo, KvState>], leader: u64) {
        for node in nodes {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let state = node.leader_state().await;
                let settled = match &state.mode {
                    LeaderMode::Leading => node.my_address() == leader,
                    LeaderMode::Following { leader: followed } => *followed == leader,
                    _ => false,
                };
                if settled {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "node {} never settled on leader {leader}, last state {state:?}",
                    node.my_address(),
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    #[tokio::test]
    async fn empty_candidate_and_bootstrap_observer_leave_vacant_owner_unclaimed() {
        let net = TestNet::new().await;
        let candidate = start_node(&net, 2, true, &[3]).await;
        let observer = SharedState::start(SharedStateConfig {
            io: net.start_io(3).await,
            my_address: 3,
            can_lead: false,
            initial_peers: vec![2],
            initial_state: KvState::default(),
            settings: fast_settings(&net.etcd, true),
        })
        .unwrap();
        let config = net.etcd.config(false);
        let mut client = etcd_client::Client::connect(config.endpoints.clone(), None)
            .await
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            assert!(client.get(config.leader_key(), None).await.unwrap().kvs().is_empty());
            assert!(matches!(candidate.leader_state().await.mode, LeaderMode::NoLeader));
            assert!(matches!(observer.leader_state().await.mode, LeaderMode::NoLeader));
            assert!(!candidate.node().eligible.load(std::sync::atomic::Ordering::Acquire));
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let bootstrap = start_node(&net, 1, true, &[2, 3]).await;
        wait_for_leader(&[&bootstrap, &candidate, &observer], 1).await;
        bootstrap.submit_action((1, 10)).await.unwrap();
        wait_for_value(&candidate, 1, 10).await;
        wait_for_value(&observer, 1, 10).await;
        assert!(candidate.node().eligible.load(std::sync::atomic::Ordering::Acquire));
    }

    #[tokio::test]
    async fn peer_partition_does_not_replace_live_etcd_owner() {
        let net = TestNet::new().await;
        let node1 = start_node(&net, 1, true, &[2]).await;
        let node2 = start_node(&net, 2, true, &[1]).await;
        wait_for_leader(&[&node1, &node2], 1).await;
        node1.submit_action((1, 1)).await.unwrap();
        wait_for_value(&node2, 1, 1).await;
        net.set_node_blocked(1, true).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(matches!(node1.leader_state().await.mode, LeaderMode::Leading));
        assert!(matches!(node2.leader_state().await.mode, LeaderMode::Following { leader: 1 }));
        net.set_node_blocked(1, false).await;
        node2.submit_action((2, 2)).await.unwrap();
        wait_for_value(&node1, 2, 2).await;
        wait_for_value(&node2, 2, 2).await;
    }

    #[tokio::test]
    async fn etcd_loss_stops_authority_while_peer_links_remain_connected() {
        let mut net = TestNet::new().await;
        let node1 = start_node(&net, 1, true, &[2]).await;
        let node2 = start_node(&net, 2, true, &[1]).await;
        wait_for_leader(&[&node1, &node2], 1).await;
        node1.submit_action((1, 1)).await.unwrap();
        wait_for_value(&node2, 1, 1).await;
        net.etcd.stop();
        tokio::time::timeout(Duration::from_secs(20), async {
            while matches!(node1.leader_state().await.mode, LeaderMode::Leading) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("authority must expire without etcd");
        let before = node1.state_handle().read_with(|state| state.state().clone());
        node1.submit_action((2, 2)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(node1.state_handle().read_with(|state| state.state().clone()), before);
        assert!(
            node1
                .node()
                .peers
                .lock()
                .await
                .get(&2)
                .unwrap()
                .connect_status
                .is_connected()
        );
    }

    #[tokio::test]
    async fn cluster_replicates_actions_from_any_node() {
        let net = TestNet::new().await;
        let node1 = start_node(&net, 1, true, &[2, 3]).await;
        let node2 = start_node(&net, 2, true, &[1, 3]).await;
        let node3 = start_node(&net, 3, false, &[1]).await;

        wait_for_leader(&[&node1, &node2, &node3], 1).await;

        /* leader applies its own action with authority */
        node1.submit_action((10, 100)).await.unwrap();
        wait_for_value(&node1, 10, 100).await;
        wait_for_value(&node2, 10, 100).await;
        wait_for_value(&node3, 10, 100).await;

        /* followers forward actions to the leader */
        node2.submit_action((20, 200)).await.unwrap();
        node3.submit_action((30, 300)).await.unwrap();
        for node in [&node1, &node2, &node3] {
            wait_for_value(node, 20, 200).await;
            wait_for_value(node, 30, 300).await;
        }
    }

    #[tokio::test]
    async fn start_recoverable_preserves_initial_recovery_details() {
        let net = TestNet::new().await;
        let io = net.start_io(1).await;

        let mut initial_state = RecoverableState::new(101, KvState::default());
        initial_state.update(&RecoverableStateAction::StateAction { action: (1, 10) });
        initial_state.update(&RecoverableStateAction::BumpGeneration { new_id: 202 });
        initial_state.update(&RecoverableStateAction::StateAction { action: (2, 20) });
        let expected_details = initial_state.details().clone();

        let node = SharedState::start_recoverable(SharedStateRecoverableConfig {
            io,
            my_address: 1,
            can_lead: true,
            initial_peers: Vec::new(),
            initial_state,
            settings: fast_settings(&net.etcd, false),
        })
        .unwrap();

        let mut handle = node.state_handle();
        let actual_details = handle.recover_details();

        assert_eq!(actual_details, expected_details);
    }

    #[tokio::test]
    async fn follower_relays_through_peer_when_leader_is_unreachable() {
        let net = TestNet::new().await;
        let node1 = start_node(&net, 1, true, &[2, 3]).await;
        let node2 = start_node(&net, 2, true, &[1, 3]).await;
        let node3 = start_node(&net, 3, false, &[1, 2]).await;

        wait_for_leader(&[&node1, &node2, &node3], 1).await;

        node1.submit_action((1, 1)).await.unwrap();
        wait_for_value(&node3, 1, 1).await;

        /* sever the direct path between the observer and the leader; node 3
         * must sync and forward actions through node 2 */
        net.set_edge_blocked(1, 3, true).await;

        node1.submit_action((2, 2)).await.unwrap();
        wait_for_value(&node3, 2, 2).await;

        node3.submit_action((3, 3)).await.unwrap();
        wait_for_value(&node1, 3, 3).await;
        wait_for_value(&node2, 3, 3).await;
        wait_for_value(&node3, 3, 3).await;
    }

    #[tokio::test]
    async fn follower_recovers_when_leader_link_goes_silent() {
        let net = TestNet::new().await;
        let node1 = start_node(&net, 1, true, &[2, 3]).await;
        let node2 = start_node(&net, 2, true, &[1, 3]).await;
        let node3 = start_node(&net, 3, false, &[1, 2]).await;

        wait_for_leader(&[&node1, &node2, &node3], 1).await;
        node1.submit_action((1, 1)).await.unwrap();
        wait_for_value(&node3, 1, 1).await;

        /* silently stall the observer's link to the leader, like a half-open
         * TCP connection: connections stay up but carry no bytes. The
         * subscription must time out instead of idling forever, and sync must
         * continue through node 2 */
        net.set_edge_blackholed(1, 3, true).await;

        node1.submit_action((2, 2)).await.unwrap();
        wait_for_value(&node3, 2, 2).await;

        /* the observer's actions must still reach the leader via the relay */
        node3.submit_action((3, 3)).await.unwrap();
        wait_for_value(&node1, 3, 3).await;
        wait_for_value(&node2, 3, 3).await;
    }

    #[tokio::test]
    async fn old_leader_rejoins_as_follower_and_its_actions_apply() {
        let net = TestNet::new().await;
        let node1 = start_node(&net, 1, true, &[2, 3]).await;
        let node2 = start_node(&net, 2, true, &[1, 3]).await;
        let node3 = start_node(&net, 3, true, &[1, 2]).await;

        wait_for_leader(&[&node1, &node2, &node3], 1).await;
        node1.submit_action((1, 1)).await.unwrap();
        wait_for_value(&node3, 1, 1).await;

        let initial_state = node1.state_handle().read_with(Clone::clone);
        net.set_node_blocked(1, true).await;
        net.stop_node(1).await;
        drop(node1);
        let successor = wait_for_common_leader(&[&node2, &node3]).await;

        node2.submit_action((2, 2)).await.unwrap();
        wait_for_value(&node3, 2, 2).await;

        net.set_node_blocked(1, false).await;
        let node1 = SharedState::start_recoverable(SharedStateRecoverableConfig {
            io: net.start_io(1).await,
            my_address: 1,
            can_lead: true,
            initial_peers: vec![2, 3],
            initial_state,
            settings: fast_settings(&net.etcd, false),
        })
        .unwrap();
        wait_for_leader(&[&node1, &node2, &node3], successor).await;
        wait_for_value(&node1, 2, 2).await;

        /* The restarted follower forwards actions to the new leader. */
        node1.submit_action((3, 3)).await.unwrap();
        wait_for_value(&node1, 3, 3).await;
        wait_for_value(&node2, 3, 3).await;
        wait_for_value(&node3, 3, 3).await;
    }

    #[tokio::test]
    async fn observer_actions_apply_after_leader_change() {
        let net = TestNet::new().await;
        let node1 = start_node(&net, 1, true, &[2, 3, 4]).await;
        let node2 = start_node(&net, 2, true, &[1, 3, 4]).await;
        let node3 = start_node(&net, 3, true, &[1, 2, 4]).await;
        let node4 = start_node(&net, 4, false, &[1, 2, 3]).await;

        wait_for_leader(&[&node1, &node2, &node3, &node4], 1).await;

        node4.submit_action((1, 1)).await.unwrap();
        wait_for_value(&node1, 1, 1).await;
        wait_for_value(&node4, 1, 1).await;

        /* leader dies; observer must move to the new leader and its actions
         * must keep applying */
        net.set_node_blocked(1, true).await;
        net.stop_node(1).await;
        drop(node1);

        wait_for_common_leader(&[&node2, &node3, &node4]).await;

        node4.submit_action((2, 2)).await.unwrap();
        wait_for_value(&node2, 2, 2).await;
        wait_for_value(&node3, 2, 2).await;
        wait_for_value(&node4, 2, 2).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn action_flood_during_failover_does_not_wedge_sync() {
        let net = TestNet::new().await;
        let node1 = Arc::new(start_node(&net, 1, true, &[2, 3]).await);
        let node2 = Arc::new(start_node(&net, 2, true, &[1, 3]).await);
        let node3 = Arc::new(start_node(&net, 3, true, &[1, 2]).await);

        let old_leader = wait_for_common_leader(&[&node1, &node2, &node3]).await;
        let (survivor_a, survivor_b) = match old_leader {
            1 => (node2.clone(), node3.clone()),
            2 => (node1.clone(), node3.clone()),
            3 => (node1.clone(), node2.clone()),
            _ => unreachable!(),
        };
        survivor_a.submit_action((9, 9)).await.unwrap();
        wait_for_value(&survivor_a, 9, 9).await;
        wait_for_value(&survivor_b, 9, 9).await;
        /* keep a continuous stream of actions flowing from both survivors
         * while the leader dies; the sync tasks must still notice the leader
         * change instead of forwarding in circles forever */
        let flood = {
            let survivor_a = survivor_a.clone();
            let survivor_b = survivor_b.clone();
            tokio::spawn(async move {
                let mut i = 0u64;
                loop {
                    let _ = survivor_a.submit_action((1000 + i, i)).await;
                    let _ = survivor_b.submit_action((2000 + i, i)).await;
                    i += 1;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        };

        net.set_node_blocked(old_leader, true).await;
        net.stop_node(old_leader).await;
        drop((node1, node2, node3));
        let new_leader = wait_for_common_leader(&[&survivor_a, &survivor_b]).await;
        flood.abort();
        let moved_follower = if survivor_a.my_address() == new_leader {
            &survivor_b
        } else {
            &survivor_a
        };

        /* after the failover flood, a fresh action from the moved follower
         * must not be wedged behind stale routing state */
        moved_follower.submit_action((1, 1)).await.unwrap();
        wait_for_value(&survivor_a, 1, 1).await;
        wait_for_value(&survivor_b, 1, 1).await;
    }

    #[tokio::test]
    async fn cluster_recovers_after_leader_failure() {
        let net = TestNet::new().await;
        let node1 = start_node(&net, 1, true, &[2, 3]).await;
        let node2 = start_node(&net, 2, true, &[1, 3]).await;
        let node3 = start_node(&net, 3, true, &[1, 2]).await;

        wait_for_leader(&[&node1, &node2, &node3], 1).await;

        node1.submit_action((1, 1)).await.unwrap();
        wait_for_value(&node2, 1, 1).await;
        wait_for_value(&node3, 1, 1).await;

        /* The surviving replicas acquire ownership and continue applying actions. */
        net.set_node_blocked(1, true).await;
        net.stop_node(1).await;
        drop(node1);

        wait_for_common_leader(&[&node2, &node3]).await;

        node3.submit_action((2, 2)).await.unwrap();
        wait_for_value(&node2, 2, 2).await;
        wait_for_value(&node3, 2, 2).await;
    }

    #[tokio::test]
    async fn five_node_cluster_replicates_from_all_nodes_after_leader_failure() {
        let net = TestNet::new().await;
        let node1 = start_node(&net, 1, true, &[2, 3, 4, 5]).await;
        let node2 = start_node(&net, 2, true, &[1, 3, 4, 5]).await;
        let node3 = start_node(&net, 3, true, &[1, 2, 4, 5]).await;
        let node4 = start_node(&net, 4, false, &[1, 2, 3, 5]).await;
        let node5 = start_node(&net, 5, false, &[1, 2, 3, 4]).await;

        let all_nodes = [&node1, &node2, &node3, &node4, &node5];
        let first_leader = wait_for_common_leader(&all_nodes).await;
        assert_eq!(first_leader, 1);

        let mut expected = BTreeMap::new();
        for node in all_nodes {
            let key = 100 + node.my_address();
            let value = key * 10;
            node.submit_action((key, value)).await.unwrap();
            expected.insert(key, value);
        }
        wait_for_cluster_state(&[&node1, &node2, &node3, &node4, &node5], &expected).await;

        net.set_node_blocked(first_leader, true).await;
        net.stop_node(first_leader).await;
        drop(node1);

        let remaining_nodes = [&node2, &node3, &node4, &node5];
        let second_leader = wait_for_common_leader(&remaining_nodes).await;
        assert_ne!(second_leader, first_leader);
        assert!([2, 3].contains(&second_leader));

        for node in remaining_nodes {
            let key = 200 + node.my_address();
            let value = key * 10;
            node.submit_action((key, value)).await.unwrap();
            expected.insert(key, value);
        }
        wait_for_cluster_state(&[&node2, &node3, &node4, &node5], &expected).await;
    }
}
