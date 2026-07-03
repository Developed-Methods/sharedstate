//! High-level entry point that provisions everything a node needs to share
//! state with a cluster: the RPC listener, peer discovery, leader election,
//! and the state sync task.

use std::{collections::HashMap, sync::Arc};

use message_encoding::MessageEncoding;
use sequenced_broadcast::{SequencedBroadcastSettings, SettingsError};
use tokio::{
    sync::{
        mpsc::{self, error::SendError},
        Mutex,
    },
    task::JoinHandle,
};

use crate::{
    cluster::{
        leader::{LeaderTask, LeaderTiming},
        node_state::{NodeState, PeerState},
        peer_connections::PeerConnections,
        peer_discovery::{PeerDiscoveryTask, PeerDiscoveryTiming},
        rpc_server::RpcServer,
        state_sync::{StateSyncTask, StateSyncTiming},
    },
    protocol::messages::{LeaderMode, LeaderState},
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

#[derive(Clone, Debug, Default)]
pub struct SharedStateSettings {
    pub net: NetIoSettings,
    pub broadcast: SequencedBroadcastSettings,
    pub discovery_timing: PeerDiscoveryTiming,
    pub leader_timing: LeaderTiming,
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

        let peers = initial_peers
            .into_iter()
            .filter(|peer| *peer != my_address)
            .map(|peer| (peer, PeerState::empty(peer)))
            .collect::<HashMap<_, _>>();

        let node = Arc::new(NodeState {
            my_address,
            can_lead,
            peers: Mutex::new(peers),
            state: SubscribableState::new(
                RecoverableState::new(unique_state_id(&my_address), initial_state),
                settings.broadcast.clone(),
            )?,
            leader_state: Mutex::new(LeaderState {
                term: 0,
                mode: LeaderMode::NoLeader,
            }),
        });

        let peer_connections = Arc::new(PeerConnections::new(io.clone(), settings.net.clone(), node.clone()));
        let (actions_tx, actions_rx) = mpsc::channel(ACTION_QUEUE_CAPACITY);
        let rpc_server = Arc::new(RpcServer::new(node.clone(), actions_tx.clone()));

        let tasks = vec![
            rpc_server.start_listener(io.clone(), settings.net.clone()),
            tokio::spawn(
                PeerDiscoveryTask::new(node.clone(), peer_connections.clone(), settings.discovery_timing).run(),
            ),
            tokio::spawn(LeaderTask::new(node.clone(), settings.leader_timing).run()),
            tokio::spawn(
                StateSyncTask::new(
                    node.clone(),
                    peer_connections,
                    io,
                    settings.net,
                    actions_rx,
                    settings.sync_timing,
                )
                .run(),
            ),
        ];

        Ok(Self { node, actions_tx, tasks })
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
        self.node.leader_state.lock().await.clone()
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
        cluster::{leader::LeaderTiming, peer_discovery::PeerDiscoveryTiming, state_sync::StateSyncTiming},
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

    fn fast_settings() -> SharedStateSettings {
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
            leader_timing: LeaderTiming {
                tick_interval: Duration::from_millis(25),
            },
            sync_timing: StateSyncTiming {
                leader_poll_interval: Duration::from_millis(20),
                retry_delay: Duration::from_millis(50),
            },
        }
    }

    async fn start_node(
        net: &SimulatedNet,
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
            settings: fast_settings(),
        })
        .unwrap()
    }

    async fn wait_for<F: FnMut() -> bool>(what: &str, mut check: F) {
        let deadline = Instant::now() + Duration::from_secs(10);
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
        wait_for(&format!("node {} to settle on {expected:?}", node.my_address()), || {
            handle.read_with(|state| {
                let state = state.state();
                state.seq == expected.len() as u64 && state.values == *expected
            })
        })
        .await;
    }

    async fn wait_for_cluster_state(nodes: &[&SharedState<SimulatedIo, KvState>], expected: &BTreeMap<u64, u64>) {
        for node in nodes {
            wait_for_state(node, expected).await;
        }
    }

    async fn wait_for_common_leader(nodes: &[&SharedState<SimulatedIo, KvState>]) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let mut observed_leader = None;
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

                if observed_leader.is_some_and(|observed| observed != leader) {
                    unsettled.push((node.my_address(), state));
                }
                observed_leader.get_or_insert(leader);
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
            let deadline = Instant::now() + Duration::from_secs(10);
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
    async fn cluster_replicates_actions_from_any_node() {
        let net = SimulatedNet::new();
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
    async fn follower_relays_through_peer_when_leader_is_unreachable() {
        let net = SimulatedNet::new();
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
    async fn old_leader_rejoins_as_follower_and_its_actions_apply() {
        let net = SimulatedNet::new();
        let node1 = start_node(&net, 1, true, &[2, 3]).await;
        let node2 = start_node(&net, 2, true, &[1, 3]).await;
        let node3 = start_node(&net, 3, true, &[1, 2]).await;

        wait_for_leader(&[&node1, &node2, &node3], 1).await;
        node1.submit_action((1, 1)).await.unwrap();
        wait_for_value(&node3, 1, 1).await;

        /* partition the leader away; the others elect node 2 */
        net.set_node_blocked(1, true).await;
        wait_for_leader(&[&node2, &node3], 2).await;

        node2.submit_action((2, 2)).await.unwrap();
        wait_for_value(&node3, 2, 2).await;

        /* heal the partition; node 1 must concede and follow node 2 */
        net.set_node_blocked(1, false).await;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let state = node1.leader_state().await;
            if matches!(state.mode, LeaderMode::Following { leader: 2 }) {
                break;
            }
            assert!(Instant::now() < deadline, "node 1 never conceded to node 2, last state {state:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        wait_for_value(&node1, 2, 2).await;

        /* the moved follower's actions must reach the new leader */
        node1.submit_action((3, 3)).await.unwrap();
        wait_for_value(&node1, 3, 3).await;
        wait_for_value(&node2, 3, 3).await;
        wait_for_value(&node3, 3, 3).await;
    }

    #[tokio::test]
    async fn observer_actions_apply_after_leader_change() {
        let net = SimulatedNet::new();
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

        wait_for_leader(&[&node2, &node3, &node4], 2).await;

        node4.submit_action((2, 2)).await.unwrap();
        wait_for_value(&node2, 2, 2).await;
        wait_for_value(&node3, 2, 2).await;
        wait_for_value(&node4, 2, 2).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn action_flood_during_failover_does_not_wedge_sync() {
        let net = SimulatedNet::new();
        let node1 = Arc::new(start_node(&net, 1, true, &[2, 3]).await);
        let node2 = Arc::new(start_node(&net, 2, true, &[1, 3]).await);
        let node3 = Arc::new(start_node(&net, 3, true, &[1, 2]).await);

        wait_for_leader(&[&node1, &node2, &node3], 1).await;

        /* keep a continuous stream of actions flowing from both survivors
         * while the leader dies; the sync tasks must still notice the leader
         * change instead of forwarding in circles forever */
        let flood = {
            let node2 = node2.clone();
            let node3 = node3.clone();
            tokio::spawn(async move {
                let mut i = 0u64;
                loop {
                    let _ = node2.submit_action((1000 + i, i)).await;
                    let _ = node3.submit_action((2000 + i, i)).await;
                    i += 1;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        };

        net.set_node_blocked(1, true).await;
        net.stop_node(1).await;
        drop(node1);

        wait_for_leader(&[&node2, &node3], 2).await;

        /* even with the flood running, a fresh action from the moved
         * follower must apply everywhere */
        node3.submit_action((1, 1)).await.unwrap();
        wait_for_value(&node2, 1, 1).await;
        wait_for_value(&node3, 1, 1).await;

        flood.abort();
    }

    #[tokio::test]
    async fn cluster_recovers_after_leader_failure() {
        let net = SimulatedNet::new();
        let node1 = start_node(&net, 1, true, &[2, 3]).await;
        let node2 = start_node(&net, 2, true, &[1, 3]).await;
        let node3 = start_node(&net, 3, true, &[1, 2]).await;

        wait_for_leader(&[&node1, &node2, &node3], 1).await;

        node1.submit_action((1, 1)).await.unwrap();
        wait_for_value(&node2, 1, 1).await;
        wait_for_value(&node3, 1, 1).await;

        /* leader goes down: the survivors elect node 2 and keep accepting
         * actions */
        net.set_node_blocked(1, true).await;
        net.stop_node(1).await;
        drop(node1);

        wait_for_leader(&[&node2, &node3], 2).await;

        node3.submit_action((2, 2)).await.unwrap();
        wait_for_value(&node2, 2, 2).await;
        wait_for_value(&node3, 2, 2).await;
    }

    #[tokio::test]
    async fn five_node_cluster_replicates_from_all_nodes_after_leader_failure() {
        let net = SimulatedNet::new();
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
        assert_eq!(second_leader, 2);

        for node in remaining_nodes {
            let key = 200 + node.my_address();
            let value = key * 10;
            node.submit_action((key, value)).await.unwrap();
            expected.insert(key, value);
        }
        wait_for_cluster_state(&[&node2, &node3, &node4, &node5], &expected).await;
    }
}
