use std::{future::Future, num::NonZeroU64, sync::Arc, time::Duration};

use futures_util::{stream, StreamExt};
use message_encoding::MessageEncoding;

use crate::{
    cluster::{
        node_state::{ConnectStatus, NodeState, PeerState},
        peer_connections::PeerConnections,
    },
    protocol::messages::{LeaderInfo, LeaderState, SharePeerDetails},
    state::deterministic_state::DeterministicState,
    transport::traits::{SyncIO, SyncIOAddress},
    utils::now_ms,
};

const RECENT_GLOBAL_CONNECTIVITY_WINDOW: Duration = Duration::from_secs(60);
const STALE_GLOBAL_CONNECTIVITY_WINDOW: Duration = Duration::from_mins(10);

const FAILED_CONNECT_RETRY_RECENT: Duration = Duration::from_secs(10);
const FAILED_CONNECT_RETRY_STALE: Duration = Duration::from_secs(30);
const FAILED_CONNECT_RETRY_OLD_OR_UNKNOWN: Duration = Duration::from_mins(30);

pub struct PeerDiscoveryTask<I: SyncIO, D: DeterministicState> {
    state: Arc<NodeState<I::Address, D>>,
    peer_connections: Arc<PeerConnections<I, D>>,
    timing: PeerDiscoveryTiming,
}

#[derive(Clone, Debug)]
pub struct PeerDiscoveryTiming {
    pub observation_interval: Duration,
    pub max_concurrent_observations: usize,
}

impl Default for PeerDiscoveryTiming {
    fn default() -> Self {
        Self {
            observation_interval: Duration::from_secs(3),
            max_concurrent_observations: 8,
        }
    }
}

impl<I, D> PeerDiscoveryTask<I, D>
where
    I: SyncIO,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    pub fn new(
        state: Arc<NodeState<I::Address, D>>,
        peer_connections: Arc<PeerConnections<I, D>>,
        timing: PeerDiscoveryTiming,
    ) -> Self {
        Self {
            state,
            peer_connections,
            timing,
        }
    }

    pub async fn run(self) {
        tracing::debug!(
            local = ?self.state.my_address,
            interval_ms = self.timing.observation_interval.as_millis(),
            max_concurrent = self.timing.max_concurrent_observations,
            "starting peer discovery task",
        );

        loop {
            self.tick().await;
            tokio::time::sleep(self.timing.observation_interval).await;
        }
    }

    pub async fn tick(&self) {
        if self.state.can_lead {
            self.broadcast_peer_details().await;
        }
        self.share_leader_info().await;
    }

    async fn broadcast_peer_details(&self) {
        self.process_data_for_peers(
            |peers| async move {
                let mut details = peers.iter().map(PeerState::share_details).collect::<Vec<_>>();
                details.push(SharePeerDetails {
                    address: self.state.my_address,
                    can_be_leader: Some(self.state.can_lead),
                    last_global_activity: NonZeroU64::new(now_ms()),
                });
                details
            },
            |peer, details| {
                let conn = self.peer_connections.clone();
                let details = details.clone();

                async move {
                    if let Ok(peers) = conn.send_peers_info(peer.addr, details).await {
                        self.state.merge_peer_details(peers).await;
                    }
                }
            },
        )
        .await;
    }

    async fn share_leader_info(&self) {
        self.process_data_for_peers(
            |peers| async move {
                LeaderInfo {
                    can_lead: self.state.can_lead,
                    leader_state: {
                        let lock = self.state.leader_state.lock().await;
                        LeaderState::clone(&*lock)
                    },
                    reachable_voters: peers.iter().filter_map(|peer| peer.connect_status.is_connected().then_some(peer.addr)).collect(),
                    recovery_details: self.state.state.recovery_details().await,
                }
            },
            |peer, leader_info| {
                let conn = self.peer_connections.clone();
                let leader_info = leader_info.clone();

                async move {
                    /* voters broadcast to everyone so observers learn the
                     * leader; observers only report their state to voters.
                     * If we don't know if peer is leader, assume it is so
                     * we can get peer discovery */
                    if !self.state.can_lead && !peer.can_lead.unwrap_or(true) {
                        return;
                    }

                    let _ = conn.send_leader_info(peer.addr, leader_info).await;
                }
            },
        )
        .await;
    }

    async fn process_data_for_peers<
        T: Future,
        M: FnOnce(Vec<PeerState<I::Address>>) -> T,
        F: Future<Output = ()>,
        A: Fn(PeerState<I::Address>, &T::Output) -> F,
    >(
        &self,
        map: M,
        action: A,
    ) {
        let peers = { self.state.peers.lock().await.values().cloned().collect::<Vec<_>>() };
        let peer_targets = peers
            .iter()
            .filter_map(|peer| should_observe_peer(peer, now_ms()).then_some(peer.clone()))
            .collect::<Vec<_>>();

        if peer_targets.is_empty() {
            tracing::debug!(local = ?self.state.my_address, "no peers to share info with");
            return;
        }

        let mapped = map(peers).await;

        let mut result_stream = stream::iter(peer_targets.into_iter().map(|peer| action(peer, &mapped)))
            .buffer_unordered(self.timing.max_concurrent_observations);

        while result_stream.next().await.is_some() {
            continue;
        }
    }
}

fn should_observe_peer<A: SyncIOAddress>(peer: &PeerState<A>, now: u64) -> bool {
    match peer.connect_status {
        ConnectStatus::Connected { .. } | ConnectStatus::NotConnected => true,
        ConnectStatus::FailedToConnect { epoch_ms } => {
            let retry_delay = failed_connect_retry_delay(peer.last_global_connectivity, now);
            now.saturating_sub(epoch_ms) >= duration_ms(retry_delay)
        }
    }
}

fn failed_connect_retry_delay(last_global_connectivity: Option<NonZeroU64>, now: u64) -> Duration {
    let Some(last_global_connectivity) = last_global_connectivity else {
        return FAILED_CONNECT_RETRY_OLD_OR_UNKNOWN;
    };

    let age = now.saturating_sub(last_global_connectivity.get());
    if age <= duration_ms(RECENT_GLOBAL_CONNECTIVITY_WINDOW) {
        FAILED_CONNECT_RETRY_RECENT
    } else if age <= duration_ms(STALE_GLOBAL_CONNECTIVITY_WINDOW) {
        FAILED_CONNECT_RETRY_STALE
    } else {
        FAILED_CONNECT_RETRY_OLD_OR_UNKNOWN
    }
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}
