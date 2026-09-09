//! Cluster coordination: peer discovery, leader election, and the RPC
//! server/client tasks that keep nodes in sync.

pub mod election;
pub mod leader;
pub mod node_state;
pub mod peer_connections;
pub mod peer_discovery;
pub mod rpc_server;
pub mod state_sync;

#[cfg(test)]
mod tests {
    use crate::{
        SharedState, SharedStateConfig, SharedStateSettings, protocol::messages::LeaderMode,
        state::deterministic_state::DeterministicState, transport::simulated::SimulatedNet,
    };
    use message_encoding::MessageEncoding;
    use std::{io, time::Duration};

    #[derive(Clone, Debug)]
    struct TestState(u64);
    impl DeterministicState for TestState {
        type Action = u64;
        type AuthorityAction = u64;
        fn accept_seq(&self) -> u64 {
            self.0
        }
        fn authority(&self, action: u64) -> u64 {
            action
        }
        fn update(&mut self, _: &u64) {
            self.0 += 1;
        }
    }
    impl MessageEncoding for TestState {
        fn write_to<T: io::Write>(&self, out: &mut T) -> io::Result<usize> {
            self.0.write_to(out)
        }
        fn read_from<T: io::Read>(input: &mut T) -> io::Result<Self> {
            Ok(Self(u64::read_from(input)?))
        }
    }

    #[tokio::test]
    async fn observer_discovers_cluster_and_leader_through_single_seed_peer() {
        let etcd = crate::test_support::TestEtcd::start().await;
        let net = SimulatedNet::new();
        let mut nodes = Vec::new();
        for (address, can_lead, initial_peers) in [(1, true, vec![2]), (2, true, vec![1]), (3, false, vec![1])] {
            nodes.push(
                SharedState::start(SharedStateConfig {
                    io: net.start_io(address).await,
                    my_address: address,
                    can_lead,
                    initial_peers,
                    initial_state: TestState(0),
                    settings: SharedStateSettings {
                        election: etcd.config(address == 1),
                        ..Default::default()
                    },
                })
                .unwrap(),
            );
        }
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let observer = nodes[2].node();
                let discovered = observer.peers.lock().await.get(&2).and_then(|peer| peer.can_lead) == Some(true);
                let reverse = nodes[1]
                    .node()
                    .peers
                    .lock()
                    .await
                    .get(&3)
                    .and_then(|peer| peer.can_lead)
                    == Some(false);
                if discovered
                    && reverse
                    && matches!(nodes[0].leader_state().await.mode, LeaderMode::Leading)
                    && matches!(nodes[1].leader_state().await.mode, LeaderMode::Following { leader: 1 })
                    && matches!(nodes[2].leader_state().await.mode, LeaderMode::Following { leader: 1 })
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("observer must discover both peers and the etcd owner");
    }
}
