use std::{collections::HashMap, num::NonZeroU64, time::Duration};

use tokio::sync::{watch, Mutex};

use crate::{
    protocol::messages::{LeaderInfo, LeaderMode, LeaderState, SharePeerDetails},
    state::{deterministic_state::DeterministicState, subscribable_state::SubscribableState},
    transport::traits::SyncIOAddress,
    utils::now_ms,
};

pub struct NodeState<A: SyncIOAddress, D: DeterministicState> {
    pub my_address: A,
    pub can_lead: bool,
    /// The local election override. Use [`Self::set_pinned_leader`] to change it at runtime.
    pub pinned_leader: Mutex<Option<A>>,
    pub peers: Mutex<HashMap<A, PeerState<A>>>,
    pub state: SubscribableState<D>,
    pub leader_state: Mutex<LeaderState<A>>,
    /// Where the local state currently gets its updates from. Published by
    /// the state sync task; the rpc server only serves subscriptions while
    /// this says the node is a live source (see [`SyncStatus::can_relay`]).
    pub sync_status: watch::Sender<SyncStatus<A>>,
}

/// How the local state is being kept up to date.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncStatus<A: SyncIOAddress> {
    /// No live source: waiting for a leader, or between subscriptions.
    NotSynced,
    /// This node has authority over the state.
    Leading,
    /// Subscribed straight to the leader's feed.
    Direct { leader: A },
    /// Subscribed to a peer that is itself subscribed to the leader.
    Relayed { relay: A, leader: A },
}

impl<A: SyncIOAddress> SyncStatus<A> {
    /// Whether other nodes may subscribe to this node's feed. Relaying is
    /// limited to one hop from the leader: a node fed through a relay never
    /// serves subscriptions itself. Otherwise nodes that are cut off from the
    /// leader can subscribe to each other and sit forever on silent feeds,
    /// never retrying the leader once it is reachable again.
    pub fn can_relay(&self) -> bool {
        matches!(self, Self::Leading | Self::Direct { .. })
    }
}

#[derive(Clone)]
pub struct PeerState<A: SyncIOAddress> {
    pub addr: A,
    pub can_lead: Option<bool>,
    pub connect_status: ConnectStatus,
    pub last_global_connectivity: Option<NonZeroU64>,
    pub leader_info: Option<LeaderInfo<A>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectStatus {
    Connected { epoch_ms: u64 },
    FailedToConnect { epoch_ms: u64 },
    NotConnected,
}

impl ConnectStatus {
    pub fn is_connected(&self) -> bool {
        matches!(self, Self::Connected { .. })
    }
}

impl<A: SyncIOAddress> PeerState<A> {
    pub(crate) fn empty(addr: A) -> Self {
        Self {
            addr,
            can_lead: None,
            connect_status: ConnectStatus::NotConnected,
            last_global_connectivity: None,
            leader_info: None,
        }
    }

    /// Whether nobody in the cluster has heard from this peer for longer
    /// than `horizon` while our own dial to it fails. Such a peer is treated
    /// as gone: it no longer counts toward the voter majority, so a cluster
    /// whose voters were replaced one by one can still elect. The entry is
    /// kept so discovery keeps retrying and a returning peer counts again as
    /// soon as it is heard from.
    ///
    /// The failed-connect timestamp is refreshed on every retry, so the age
    /// comes from `last_global_connectivity` instead. A peer nobody has ever
    /// heard from has no age and never expires.
    pub fn is_expired(&self, now_ms: u64, horizon: Duration) -> bool {
        let Some(last_seen) = self.last_global_connectivity else {
            return false;
        };
        matches!(self.connect_status, ConnectStatus::FailedToConnect { .. })
            && duration_ms(horizon) < now_ms.saturating_sub(last_seen.get())
    }

    pub(crate) fn share_details(&self) -> SharePeerDetails<A> {
        SharePeerDetails {
            address: self.addr,
            can_be_leader: self.can_lead,
            last_global_activity: self.last_global_connectivity,
        }
    }
}

impl<A, D> NodeState<A, D>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    /// Overrides elections locally. Apply the same pin to every node in the cluster.
    /// The selected address must belong to a voter (`can_lead = true`).
    /// Unknown addresses are discovered automatically; known observers cannot become leaders.
    /// A pin disables quorum checks and automatic failover, even if the leader becomes unreachable.
    /// Conflicting pins can create independent leaders and divergent state.
    /// Clearing an existing pin with `None` starts a new consensus election.
    pub async fn set_pinned_leader(&self, leader: Option<A>) {
        let mut pinned = self.pinned_leader.lock().await;
        if *pinned == leader {
            return;
        }
        if let Some(address) = leader.filter(|address| *address != self.my_address) {
            self.peers
                .lock()
                .await
                .entry(address)
                .or_insert_with(|| PeerState::empty(address));
        }
        let mode = match leader {
            Some(address) => self.pinned_leader_mode(address).await,
            None if self.can_lead => LeaderMode::Electing { vote: None },
            None => LeaderMode::NoLeader,
        };
        let mut state = self.leader_state.lock().await;
        *pinned = leader;
        state.term = state.term.bump();
        state.mode = mode;
        tracing::info!(?leader, state = ?*state, "leader pin updated");
    }

    pub(crate) async fn pinned_leader_mode(&self, leader: A) -> LeaderMode<A> {
        if leader == self.my_address {
            return if self.can_lead {
                LeaderMode::Leading
            } else {
                LeaderMode::NoLeader
            };
        }
        let peers = self.peers.lock().await;
        let can_lead = peers.get(&leader).and_then(|peer| {
            peer.can_lead
                .or_else(|| peer.leader_info.as_ref().map(|info| info.can_lead))
        });
        if can_lead == Some(false) {
            LeaderMode::NoLeader
        } else {
            LeaderMode::Following { leader }
        }
    }

    pub(crate) async fn merge_peer_details(&self, shared_peers: Vec<SharePeerDetails<A>>) {
        let mut peers = self.peers.lock().await;

        for shared in shared_peers {
            if shared.address == self.my_address {
                continue;
            }

            let peer_state = peers
                .entry(shared.address)
                .or_insert_with(|| PeerState::empty(shared.address));

            if let Some(can_lead) = shared.can_be_leader {
                peer_state.can_lead = Some(can_lead);
            }

            peer_state.last_global_connectivity =
                merge_last_activity(peer_state.last_global_connectivity, shared.last_global_activity);
        }
    }

    pub(crate) async fn known_peer_details(&self) -> Vec<SharePeerDetails<A>> {
        self.peers.lock().await.values().map(PeerState::share_details).collect()
    }

    /// Records activity from a peer, registering it if this is first contact.
    /// Returns whether the peer was already known.
    pub(crate) async fn note_known_peer_activity(&self, peer: A) -> bool {
        let mut peers = self.peers.lock().await;
        let known = peers.contains_key(&peer);
        let peer_state = peers.entry(peer).or_insert_with(|| PeerState::empty(peer));
        peer_state.last_global_connectivity = NonZeroU64::new(now_ms());
        known
    }

    pub(crate) async fn mark_peer_connected(&self, peer: A) {
        self.set_peer_connect_status(peer, ConnectStatus::Connected { epoch_ms: now_ms() })
            .await;
    }

    pub(crate) async fn mark_peer_not_connected(&self, peer: A) {
        self.set_peer_connect_status(peer, ConnectStatus::NotConnected).await;
    }

    pub(crate) async fn mark_peer_failed_to_connect(&self, peer: A) {
        self.set_peer_connect_status(peer, ConnectStatus::FailedToConnect { epoch_ms: now_ms() })
            .await;
    }

    async fn set_peer_connect_status(&self, peer: A, connect_status: ConnectStatus) {
        self.peers
            .lock()
            .await
            .entry(peer)
            .and_modify(|peer_state| {
                peer_state.connect_status = connect_status;
            })
            .or_insert_with(|| {
                let mut peer_state = PeerState::empty(peer);
                peer_state.connect_status = connect_status;
                peer_state
            });
    }
}

fn merge_last_activity(current: Option<NonZeroU64>, incoming: Option<NonZeroU64>) -> Option<NonZeroU64> {
    match (current, incoming) {
        (None, Some(activity)) | (Some(activity), None) => Some(activity),
        (Some(a), Some(b)) => Some(a.max(b)),
        (None, None) => None,
    }
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    const HORIZON: Duration = Duration::from_secs(60 * 60);
    const NOW: u64 = 10 * 60 * 60 * 1000;

    fn peer(connect_status: ConnectStatus, last_seen_ms: Option<u64>) -> PeerState<u16> {
        PeerState {
            addr: 1,
            can_lead: Some(true),
            connect_status,
            last_global_connectivity: last_seen_ms.and_then(NonZeroU64::new),
            leader_info: None,
        }
    }

    fn ms(duration: Duration) -> u64 {
        duration.as_millis() as u64
    }

    #[test]
    fn expires_once_unheard_for_longer_than_horizon() {
        let failed = ConnectStatus::FailedToConnect { epoch_ms: NOW };
        assert!(peer(failed, Some(NOW - ms(HORIZON) - 1)).is_expired(NOW, HORIZON));
        assert!(!peer(failed, Some(NOW - ms(HORIZON))).is_expired(NOW, HORIZON));
        assert!(!peer(failed, Some(NOW)).is_expired(NOW, HORIZON));
    }

    #[test]
    fn only_expires_while_dialing_fails() {
        let long_ago = Some(NOW - 2 * ms(HORIZON));
        assert!(!peer(ConnectStatus::Connected { epoch_ms: NOW }, long_ago).is_expired(NOW, HORIZON));
        assert!(!peer(ConnectStatus::NotConnected, long_ago).is_expired(NOW, HORIZON));
    }

    #[test]
    fn never_heard_from_does_not_expire() {
        let failed = ConnectStatus::FailedToConnect { epoch_ms: 0 };
        assert!(!peer(failed, None).is_expired(NOW, HORIZON));
    }

    #[test]
    fn refreshed_failure_timestamp_does_not_reset_age() {
        /* discovery redials and rewrites the failure epoch every retry; the
         * peer must still count as expired */
        let failed = ConnectStatus::FailedToConnect { epoch_ms: NOW };
        assert!(peer(failed, Some(NOW - 2 * ms(HORIZON))).is_expired(NOW, HORIZON));
    }
}
