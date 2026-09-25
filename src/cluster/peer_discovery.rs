use std::{
    future::Future,
    num::NonZeroU64,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use futures_util::{stream, StreamExt};
use message_encoding::MessageEncoding;

use crate::{
    cluster::{
        node_state::{ConnectStatus, GatewayView, NodeState, PeerState},
        peer_connections::{PeerConnections, PeerRpcError},
    },
    state::deterministic_state::DeterministicState,
    transport::traits::{SyncIO, SyncIOAddress},
    utils::now_ms,
};

const RECENT_GLOBAL_CONNECTIVITY_WINDOW: Duration = Duration::from_secs(60);
const STALE_GLOBAL_CONNECTIVITY_WINDOW: Duration = Duration::from_mins(10);

const FAILED_CONNECT_RETRY_RECENT: Duration = Duration::from_secs(10);
const FAILED_CONNECT_RETRY_STALE: Duration = Duration::from_secs(30);
const FAILED_CONNECT_RETRY_OLD_OR_UNKNOWN: Duration = Duration::from_mins(30);

/// The pooled rpc connection to the gateway pins us to one voter. If that
/// voter keeps reporting no leader while the rest of the cluster may have
/// elected one, drop the connection after this many ticks so the next dial
/// can land elsewhere.
const GATEWAY_NO_LEADER_REDIAL_TICKS: u32 = 3;

pub struct PeerDiscoveryTask<I: SyncIO, D: DeterministicState> {
    state: Arc<NodeState<I::Address, D>>,
    peer_connections: Arc<PeerConnections<I, D>>,
    timing: PeerDiscoveryTiming,
    gateway_no_leader_ticks: AtomicU32,
}

#[derive(Clone, Debug)]
pub struct PeerDiscoveryTiming {
    pub observation_interval: Duration,
    pub max_concurrent_observations: usize,
    /// How long the last answer from the voter gateway keeps counting as a
    /// connected voter. Discovery refreshes it every tick, so this only
    /// needs to cover one slow tick; it must exceed `observation_interval`
    /// plus the rpc timeouts a tick can spend on the gateway.
    pub gateway_view_ttl: Duration,
}

impl Default for PeerDiscoveryTiming {
    fn default() -> Self {
        Self {
            observation_interval: Duration::from_secs(3),
            max_concurrent_observations: 8,
            gateway_view_ttl: Duration::from_secs(60),
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
            gateway_no_leader_ticks: AtomicU32::new(0),
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
        /* nobody dials an inaccessible node to push peers or leader info,
         * so it pulls both itself */
        if self.state.can_lead || !self.state.accessible {
            self.broadcast_peer_details().await;
        }
        if self.state.accessible {
            self.share_leader_info().await;
        } else {
            self.pull_leader_info().await;
        }
        if let Some(gateway) = self.state.voter_gateway {
            self.observe_gateway(gateway).await;
        }
    }

    /// Whether we dial this peer ourselves. With a gateway configured the
    /// voters sit behind it and cannot be reached directly, so they are
    /// only ever talked to through the gateway.
    fn dials_directly(&self, peer: &PeerState<I::Address>) -> bool {
        self.state.voter_gateway.is_none() || peer.can_lead != Some(true)
    }

    /// One round trip of everything a direct peer would get pushed, sent
    /// through the gateway to whichever voter answers, plus a leader query
    /// that becomes our view of the voter set.
    async fn observe_gateway(&self, gateway: I::Address) {
        let peers = { self.state.peers.lock().await.values().cloned().collect::<Vec<_>>() };

        let mut details = peers.iter().map(PeerState::share_details).collect::<Vec<_>>();
        details.extend(self.state.my_peer_details());
        match self.peer_connections.send_peers_info(gateway, details).await {
            Ok(shared) => self.state.merge_peer_details(shared).await,
            Err(error) => return self.lose_gateway(gateway, "share peers", error).await,
        }

        /* voters drop leader info from inaccessible clients unread */
        if self.state.accessible {
            let info = self.state.leader_info().await;
            if let Err(error) = self.peer_connections.send_leader_info(gateway, info).await {
                return self.lose_gateway(gateway, "share leader info", error).await;
            }
        }

        let answer = match self.peer_connections.query_leader(gateway).await {
            Ok(answer) => answer,
            Err(error) => return self.lose_gateway(gateway, "query leader", error).await,
        };

        let view = GatewayView::from_voter_state(&answer, now_ms() + duration_ms(self.timing.gateway_view_ttl));
        if view.has_leader {
            self.gateway_no_leader_ticks.store(0, Ordering::Relaxed);
        } else if GATEWAY_NO_LEADER_REDIAL_TICKS <= self.gateway_no_leader_ticks.fetch_add(1, Ordering::Relaxed) + 1 {
            tracing::info!(?gateway, ?answer, "voter behind the gateway keeps reporting no leader, redialing");
            self.gateway_no_leader_ticks.store(0, Ordering::Relaxed);
            self.peer_connections.kill_connection(gateway).await;
        }

        let mut current = self.state.gateway_view.lock().await;
        if current.as_ref().map(|current| (current.term, current.has_leader)) != Some((view.term, view.has_leader)) {
            tracing::info!(?gateway, ?answer, "gateway view updated");
        }
        *current = Some(view);
    }

    async fn lose_gateway(&self, gateway: I::Address, step: &'static str, error: PeerRpcError) {
        tracing::warn!(?gateway, step, ?error, "voter gateway rpc failed");
        self.gateway_no_leader_ticks.store(0, Ordering::Relaxed);
        *self.state.gateway_view.lock().await = None;
    }

    async fn broadcast_peer_details(&self) {
        self.process_data_for_peers(
            |peers| async move {
                let mut details = peers.iter().map(PeerState::share_details).collect::<Vec<_>>();
                details.extend(self.state.my_peer_details());
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
            |_| async move { self.state.leader_info().await },
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

    /// The pull counterpart of the leader info voters push to everyone they
    /// dial. Only voters' info matters to an observer's election rules, and
    /// a peer whose role is unknown is asked in case it is one.
    async fn pull_leader_info(&self) {
        self.process_data_for_peers(
            |_| async {},
            |peer, _| {
                let conn = self.peer_connections.clone();

                async move {
                    if !peer.can_lead.unwrap_or(true) {
                        return;
                    }

                    if let Ok(info) = conn.query_leader_info(peer.addr).await {
                        self.state.record_leader_info(peer.addr, info).await;
                    }
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
            .filter(|peer| self.dials_directly(peer))
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
