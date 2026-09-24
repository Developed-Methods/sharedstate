//! Cluster coordination: peer discovery, leader election, and the RPC
//! server/client tasks that keep nodes in sync.

pub mod leader;
pub mod node_state;
pub mod peer_connections;
pub mod peer_discovery;
pub mod rpc_server;
pub mod state_sync;

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Result, sync::Arc};

    use message_encoding::MessageEncoding;
    use sequenced_broadcast::SequencedBroadcastSettings;
    use tokio::sync::{mpsc, watch, Mutex};

    use crate::{
        cluster::{
            leader::{LeaderMode, LeaderTask, LeaderTiming, PeerExpiry},
            node_state::{NodeState, PeerState, SyncStatus},
            peer_connections::PeerConnections,
            peer_discovery::{PeerDiscoveryTask, PeerDiscoveryTiming},
            rpc_server::RpcServer,
        },
        protocol::messages::{ElectionTerm, LeaderState},
        state::{
            deterministic_state::DeterministicState, recoverable_state::RecoverableState,
            subscribable_state::SubscribableState,
        },
        transport::{
            channels::NetIoSettings,
            simulated::{SimulatedIo, SimulatedNet},
        },
    };

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub(super) struct TestState(pub u64);

    impl DeterministicState for TestState {
        type Action = u64;
        type AuthorityAction = u64;

        fn accept_seq(&self) -> u64 {
            self.0
        }

        fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
            action
        }

        fn update(&mut self, _action: &Self::AuthorityAction) {
            self.0 += 1;
        }
    }

    impl MessageEncoding for TestState {
        fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
            self.0.write_to(out)
        }

        fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
            Ok(Self(MessageEncoding::read_from(read)?))
        }
    }

    struct TestNode {
        state: Arc<NodeState<u64, TestState>>,
        discovery: PeerDiscoveryTask<SimulatedIo, TestState>,
        leader: LeaderTask<u64, TestState>,
        _actions_rx: mpsc::Receiver<(u64, u64)>,
    }

    impl TestNode {
        async fn start(net: &SimulatedNet, addr: u64, can_lead: bool, initial_peers: &[u64]) -> Self {
            let io = net.start_io(addr).await;
            let settings = NetIoSettings::default();

            let peers = initial_peers
                .iter()
                .map(|peer| (*peer, PeerState::empty(*peer)))
                .collect::<HashMap<_, _>>();

            let state = Arc::new(NodeState {
                my_address: addr,
                can_lead,
                voter_gateway: None,
                gateway_view: Mutex::new(None),
                peers: Mutex::new(peers),
                state: SubscribableState::new(
                    RecoverableState::new(addr, TestState(0)),
                    SequencedBroadcastSettings::default(),
                )
                .unwrap(),
                leader_state: Mutex::new(LeaderState {
                    term: ElectionTerm::from_term(0),
                    mode: LeaderMode::NoLeader,
                }),
                sync_status: watch::Sender::new(SyncStatus::NotSynced),
            });

            let connections = Arc::new(PeerConnections::new(io.clone(), settings.clone(), state.clone()));
            let (actions_tx, actions_rx) = mpsc::channel(16);
            let rpc_server = Arc::new(RpcServer::new(state.clone(), actions_tx));
            rpc_server.start_listener(io, settings);

            Self {
                state: state.clone(),
                discovery: PeerDiscoveryTask::new(state.clone(), connections, PeerDiscoveryTiming::default()),
                leader: LeaderTask::new(state, LeaderTiming::default(), PeerExpiry::default()),
                _actions_rx: actions_rx,
            }
        }

        async fn leader_mode(&self) -> LeaderMode<u64> {
            self.state.leader_state.lock().await.mode.clone()
        }
    }

    #[tokio::test]
    async fn observer_discovers_cluster_and_leader_through_single_seed_peer() {
        let net = SimulatedNet::new();

        /* voters know each other; the observer only knows voter 1 */
        let mut nodes = [
            TestNode::start(&net, 1, true, &[2]).await,
            TestNode::start(&net, 2, true, &[1]).await,
            TestNode::start(&net, 3, false, &[1]).await,
        ];

        for _ in 0..20 {
            for node in nodes.iter_mut() {
                node.discovery.tick().await;
                node.leader.tick().await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        assert_eq!(nodes[0].leader_mode().await, LeaderMode::Leading);
        assert_eq!(nodes[1].leader_mode().await, LeaderMode::Following { leader: 1 });

        /* the observer found the leader and discovered voter 2 via gossip */
        assert_eq!(nodes[2].leader_mode().await, LeaderMode::Following { leader: 1 });
        let observer_peers = nodes[2].state.peers.lock().await;
        assert!(observer_peers.contains_key(&2), "observer never discovered voter 2");
        assert_eq!(observer_peers.get(&2).unwrap().can_lead, Some(true));

        /* and voter 2, which never had the observer configured, learned of it */
        let voter_peers = nodes[1].state.peers.lock().await;
        assert_eq!(voter_peers.get(&3).and_then(|peer| peer.can_lead), Some(false));
    }
}

#[cfg(test)]
mod dead_voter_expiry_tests {
    use std::{
        collections::HashMap,
        num::NonZeroU64,
        sync::Arc,
        time::{Duration, Instant},
    };

    use sequenced_broadcast::SequencedBroadcastSettings;
    use tokio::sync::{mpsc, watch, Mutex};

    use crate::{
        cluster::{
            leader::{LeaderMode, LeaderTask, LeaderTiming, PeerExpiry},
            node_state::{ConnectStatus, NodeState, PeerState, SyncStatus},
            peer_connections::PeerConnections,
            peer_discovery::{PeerDiscoveryTask, PeerDiscoveryTiming},
            rpc_server::RpcServer,
        },
        protocol::messages::{ElectionTerm, LeaderState},
        state::{recoverable_state::RecoverableState, subscribable_state::SubscribableState},
        transport::{
            channels::NetIoSettings,
            simulated::{SimulatedIo, SimulatedNet},
        },
        utils::now_ms,
    };

    use super::tests::TestState;

    /// How long each replaced voter has been gone before the next
    /// replacement happens. Far beyond any sensible expiry horizon, so a
    /// fix with any reasonable default will pass this test.
    const TIME_SINCE_VOTER_DIED: Duration = Duration::from_secs(24 * 60 * 60);

    const STAGE_TIMEOUT: Duration = Duration::from_secs(15);

    struct Voter {
        addr: u64,
        state: Arc<NodeState<u64, TestState>>,
        discovery: PeerDiscoveryTask<SimulatedIo, TestState>,
        leader: LeaderTask<u64, TestState>,
        _actions_rx: mpsc::Receiver<(u64, u64)>,
    }

    impl Voter {
        async fn start(net: &SimulatedNet, addr: u64, initial_peers: &[u64]) -> Self {
            let io = net.start_io(addr).await;
            let settings = NetIoSettings {
                process_timeout: Duration::from_millis(500),
                message_timeout: Duration::from_secs(1),
            };

            let peers = initial_peers
                .iter()
                .map(|peer| (*peer, PeerState::empty(*peer)))
                .collect::<HashMap<_, _>>();

            let state = Arc::new(NodeState {
                my_address: addr,
                can_lead: true,
                voter_gateway: None,
                gateway_view: Mutex::new(None),
                peers: Mutex::new(peers),
                state: SubscribableState::new(
                    RecoverableState::new(addr, TestState(0)),
                    SequencedBroadcastSettings::default(),
                )
                .unwrap(),
                leader_state: Mutex::new(LeaderState {
                    term: ElectionTerm::from_term(0),
                    mode: LeaderMode::NoLeader,
                }),
                sync_status: watch::Sender::new(SyncStatus::NotSynced),
            });

            let connections = Arc::new(PeerConnections::new(io.clone(), settings.clone(), state.clone()));
            let (actions_tx, actions_rx) = mpsc::channel(16);
            let rpc_server = Arc::new(RpcServer::new(state.clone(), actions_tx));
            rpc_server.start_listener(io, settings);

            Self {
                addr,
                state: state.clone(),
                discovery: PeerDiscoveryTask::new(state.clone(), connections, PeerDiscoveryTiming::default()),
                leader: LeaderTask::new(state, LeaderTiming::default(), PeerExpiry::default()),
                _actions_rx: actions_rx,
            }
        }

        async fn leader_state(&self) -> LeaderState<u64> {
            self.state.leader_state.lock().await.clone()
        }
    }

    async fn tick_all(voters: &mut [Voter]) {
        for voter in voters.iter_mut() {
            voter.discovery.tick().await;
            voter.leader.tick().await;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    /// The leader every live voter agrees on at a common term, if any.
    async fn agreed_leader(voters: &[Voter]) -> Option<u64> {
        let mut agreed = None;
        for voter in voters {
            let state = voter.leader_state().await;
            let leader = match &state.mode {
                LeaderMode::Leading => voter.addr,
                LeaderMode::Following { leader } => *leader,
                _ => return None,
            };
            match agreed {
                None => agreed = Some((leader, state.term)),
                Some(seen) if seen != (leader, state.term) => return None,
                Some(_) => {}
            }
        }
        agreed.map(|(leader, _)| leader)
    }

    async fn leader_states(voters: &[Voter]) -> Vec<(u64, LeaderState<u64>)> {
        let mut states = Vec::new();
        for voter in voters {
            states.push((voter.addr, voter.leader_state().await));
        }
        states
    }

    async fn run_until_leader(voters: &mut [Voter], stage: &str) -> u64 {
        let deadline = Instant::now() + STAGE_TIMEOUT;
        loop {
            tick_all(voters).await;
            if let Some(leader) = agreed_leader(voters).await {
                return leader;
            }
            assert!(
                Instant::now() < deadline,
                "{stage}: live voters never agreed on a leader, states {:?}",
                leader_states(voters).await,
            );
        }
    }

    /// Waits until every live voter has given up dialing `dead`.
    async fn run_until_unreachable(voters: &mut [Voter], dead: u64) {
        let deadline = Instant::now() + STAGE_TIMEOUT;
        loop {
            tick_all(voters).await;
            let mut all_failed = true;
            for voter in voters.iter() {
                let peers = voter.state.peers.lock().await;
                let status = peers.get(&dead).map(|peer| peer.connect_status);
                all_failed &= matches!(status, Some(ConnectStatus::FailedToConnect { .. }));
            }
            if all_failed {
                return;
            }
            assert!(Instant::now() < deadline, "live voters never marked {dead} as failed to connect");
        }
    }

    /// Pretends `age` has passed since every unreachable peer was last
    /// seen. The peer maps only ever hold wall-clock timestamps, so this is
    /// the only way to make an outage look old without sleeping for a day.
    async fn age_unreachable_peers(voters: &[Voter], age: Duration) {
        let then = now_ms().saturating_sub(age.as_millis() as u64);
        for voter in voters {
            let mut peers = voter.state.peers.lock().await;
            for peer in peers.values_mut() {
                if let ConnectStatus::FailedToConnect { epoch_ms } = &mut peer.connect_status {
                    *epoch_ms = then;
                    peer.last_global_connectivity = NonZeroU64::new(then);
                }
            }
        }
    }

    /// Stops `dead`, waits until the survivors have given up on it, lets a
    /// day pass, then brings up `replacement` seeded with the survivors.
    async fn replace_voter(net: &SimulatedNet, voters: &mut Vec<Voter>, dead: u64, replacement: u64) {
        let index = voters.iter().position(|voter| voter.addr == dead).unwrap();
        drop(voters.remove(index));
        net.stop_node(dead).await;

        run_until_unreachable(voters, dead).await;
        age_unreachable_peers(voters, TIME_SINCE_VOTER_DIED).await;

        let survivors = voters.iter().map(|voter| voter.addr).collect::<Vec<_>>();
        voters.push(Voter::start(net, replacement, &survivors).await);
    }

    /// A two voter cluster loses one voter. While the outage is shorter than
    /// the horizon the survivor must keep waiting: one of two known voters is
    /// not a majority, and the other side of a partition would look exactly
    /// the same. Past the horizon the survivor leads alone, and when the
    /// voter comes back it counts again and joins.
    #[tokio::test]
    async fn short_outage_blocks_election_and_long_outage_does_not() {
        let horizon = PeerExpiry::default().voter_horizon;

        let net = SimulatedNet::new();
        let mut voters = vec![Voter::start(&net, 1, &[2]).await, Voter::start(&net, 2, &[1]).await];
        assert_eq!(run_until_leader(&mut voters, "initial cluster").await, 1);

        drop(voters.remove(1));
        net.stop_node(2).await;
        run_until_unreachable(&mut voters, 2).await;

        age_unreachable_peers(&voters, horizon / 2).await;
        for _ in 0..20 {
            tick_all(&mut voters).await;
        }
        assert!(
            matches!(voters[0].leader_state().await.mode, LeaderMode::Electing { .. }),
            "survivor claimed leadership during a short outage, state {:?}",
            voters[0].leader_state().await
        );

        age_unreachable_peers(&voters, horizon * 2).await;
        assert_eq!(run_until_leader(&mut voters, "after the outage outlasts the horizon").await, 1);

        voters.push(Voter::start(&net, 2, &[1]).await);
        assert_eq!(run_until_leader(&mut voters, "after voter 2 returns").await, 1);

        let peers = voters[0].state.peers.lock().await;
        assert!(
            !peers[&2].is_expired(now_ms(), horizon),
            "returned voter is still treated as expired: {:?}",
            peers[&2].connect_status
        );
    }

    /// Replacing every voter of a three node cluster one at a time, with a
    /// day between replacements, must leave a working cluster.
    ///
    /// Every voter that ever existed stays in the peer maps as a voter and
    /// is gossiped to the replacements that never met it. Without expiry the
    /// third replacement leaves three live voters facing six known voters,
    /// which is not a strict majority, and the election never completes.
    #[tokio::test]
    async fn rolling_voter_replacement_does_not_stall_election() {
        let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

        let net = SimulatedNet::new();
        let mut voters = vec![
            Voter::start(&net, 1, &[2, 3]).await,
            Voter::start(&net, 2, &[1, 3]).await,
            Voter::start(&net, 3, &[1, 2]).await,
        ];
        assert_eq!(run_until_leader(&mut voters, "initial cluster").await, 1);

        replace_voter(&net, &mut voters, 1, 4).await;
        assert_eq!(run_until_leader(&mut voters, "after replacing voter 1").await, 2);

        replace_voter(&net, &mut voters, 2, 5).await;
        assert_eq!(run_until_leader(&mut voters, "after replacing voter 2").await, 3);

        replace_voter(&net, &mut voters, 3, 6).await;

        /* the replacement gossips with the survivors and learns about the
         * dead voters it never met, then fails to dial them itself */
        run_until_unreachable(&mut voters, 1).await;
        age_unreachable_peers(&voters, TIME_SINCE_VOTER_DIED).await;

        assert_eq!(run_until_leader(&mut voters, "after replacing voter 3").await, 4);
    }
}
