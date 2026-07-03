//! Cluster coordination: peer discovery, leader election, and the RPC
//! server/client tasks that keep nodes in sync.

pub mod leader;
pub mod node_state;
pub mod peer_connections;
pub mod peer_discovery;
pub mod rpc_server;

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Result, sync::Arc};

    use message_encoding::MessageEncoding;
    use sequenced_broadcast::SequencedBroadcastSettings;
    use tokio::sync::{mpsc, Mutex};

    use crate::{
        cluster::{
            leader::{LeaderMode, LeaderTask, LeaderTiming},
            node_state::{NodeState, PeerState},
            peer_connections::PeerConnections,
            peer_discovery::{PeerDiscoveryTask, PeerDiscoveryTiming},
            rpc_server::RpcServer,
        },
        protocol::messages::LeaderState,
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
    struct TestState(u64);

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
                peers: Mutex::new(peers),
                state: SubscribableState::new(
                    RecoverableState::new(addr, TestState(0)),
                    SequencedBroadcastSettings::default(),
                )
                .unwrap(),
                leader_state: Mutex::new(LeaderState {
                    term: 0,
                    mode: LeaderMode::NoLeader,
                }),
            });

            let connections = Arc::new(PeerConnections::new(io.clone(), settings.clone(), state.clone()));
            let (actions_tx, actions_rx) = mpsc::channel(16);
            let rpc_server = Arc::new(RpcServer::new(state.clone(), actions_tx));
            rpc_server.start_listener(io, settings);

            Self {
                state: state.clone(),
                discovery: PeerDiscoveryTask::new(state.clone(), connections, PeerDiscoveryTiming::default()),
                leader: LeaderTask::new(state, LeaderTiming::default()),
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
