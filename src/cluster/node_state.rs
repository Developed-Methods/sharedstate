use std::{collections::HashMap, num::NonZeroU64};

use tokio::sync::Mutex;

use crate::{
    protocol::messages::{LeaderInfo, LeaderState, SharePeerDetails},
    state::{deterministic_state::DeterministicState, subscribable_state::SubscribableState},
    transport::traits::SyncIOAddress,
    utils::now_ms,
};

pub struct NodeState<A: SyncIOAddress, D: DeterministicState> {
    pub my_address: A,
    pub can_lead: bool,
    pub peers: Mutex<HashMap<A, PeerState<A>>>,
    pub state: SubscribableState<D>,
    pub leader_state: Mutex<LeaderState<A>>,
}

#[derive(Clone)]
pub struct PeerState<A: SyncIOAddress> {
    pub addr: A,
    pub can_lead: Option<bool>,
    pub connect_status: ConnectStatus,
    pub last_global_connectivity: Option<NonZeroU64>,
    pub leader_info: Option<LeaderInfo<A>>,
    pub reachable_voters: Vec<A>,
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
            reachable_voters: Vec::new(),
        }
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
