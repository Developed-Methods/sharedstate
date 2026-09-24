use std::{collections::HashMap, num::NonZeroU64, time::Duration};

use tokio::sync::{Mutex, watch};

use crate::{
    protocol::messages::{ElectionTerm, LeaderInfo, LeaderMode, LeaderState, SharePeerDetails},
    state::{deterministic_state::DeterministicState, subscribable_state::SubscribableState},
    transport::traits::SyncIOAddress,
    utils::now_ms,
};

pub struct NodeState<A: SyncIOAddress, D: DeterministicState> {
    pub my_address: A,
    pub can_lead: bool,
    /// An address that reaches some voter, for observers that cannot dial
    /// voters individually (a load balancer in front of the voter set). It
    /// is a dial target only: never a peer identity, never stored in
    /// `peers`, never gossiped. Observers with a gateway treat the voter
    /// set as one virtual leader living at this address.
    pub voter_gateway: Option<A>,
    /// What the voter answering behind the gateway last reported about the
    /// election, published by peer discovery. `None` until the gateway
    /// answers, and again whenever an rpc to it fails.
    pub gateway_view: Mutex<Option<GatewayView>>,
    pub peers: Mutex<HashMap<A, PeerState<A>>>,
    pub state: SubscribableState<D>,
    pub leader_state: Mutex<LeaderState<A>>,
    /// Where the local state currently gets its updates from. Published by
    /// the state sync task; the rpc server only serves subscriptions while
    /// this says the node is a live source (see [`SyncStatus::can_relay`]).
    pub sync_status: watch::Sender<SyncStatus<A>>,
}

/// The election state of the voter set as seen through the gateway.
///
/// Whichever voter answers, the observer only needs to know whether the
/// cluster has a leader and at what term, so `Following` answers are
/// rewritten to `Leading` before being stored (see
/// [`GatewayView::from_voter_state`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayView {
    pub term: ElectionTerm,
    pub has_leader: bool,
    /// Wall-clock deadline after which the view no longer counts as a
    /// connected voter; discovery refreshes it every tick.
    pub valid_until_ms: u64,
}

impl GatewayView {
    pub fn from_voter_state<A: SyncIOAddress>(state: &LeaderState<A>, valid_until_ms: u64) -> Self {
        Self {
            term: state.term,
            has_leader: matches!(state.mode, LeaderMode::Leading | LeaderMode::Following { .. }),
            valid_until_ms,
        }
    }

    /// The view as a voter's leader state at the gateway address, for the
    /// observer election rules which only look at connected voters.
    pub fn as_leader_state<A: SyncIOAddress>(&self) -> LeaderState<A> {
        LeaderState {
            term: self.term,
            mode: if self.has_leader {
                LeaderMode::Leading
            } else {
                LeaderMode::Electing { vote: None }
            },
        }
    }

    pub fn is_fresh(&self, now_ms: u64) -> bool {
        now_ms <= self.valid_until_ms
    }
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
    /// Subscribed through the voter gateway to whichever voter answered;
    /// that voter is the leader or fed directly by it.
    Gateway { gateway: A },
    /// Subscribed to a peer that is itself subscribed to the leader.
    Relayed { relay: A, leader: A },
}

impl<A: SyncIOAddress> SyncStatus<A> {
    /// Whether other nodes may subscribe to this node's feed. Relaying is
    /// limited to one hop from a voter source (the leader itself, a voter fed
    /// by it, or the voter set behind a gateway): a node fed through a relay
    /// never serves subscriptions itself. Otherwise nodes that are cut off
    /// from the leader can subscribe to each other and sit forever on silent
    /// feeds, never retrying the leader once it is reachable again.
    pub fn can_relay(&self) -> bool {
        matches!(self, Self::Leading | Self::Direct { .. } | Self::Gateway { .. })
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
    /// Whether `addr` is the voter gateway, which must never enter the peer
    /// map: it is not a node, and gossiping it would make voters count it
    /// as one of their own.
    pub fn is_gateway(&self, addr: A) -> bool {
        self.voter_gateway == Some(addr)
    }

    pub(crate) async fn merge_peer_details(&self, shared_peers: Vec<SharePeerDetails<A>>) {
        let mut peers = self.peers.lock().await;

        for shared in shared_peers {
            if shared.address == self.my_address || self.is_gateway(shared.address) {
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
        if self.is_gateway(peer) {
            return true;
        }
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
        if self.is_gateway(peer) {
            return;
        }
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
    fn gateway_view_only_keeps_whether_the_cluster_has_a_leader() {
        let term = ElectionTerm::from_term(4);
        let leading = LeaderState::<u16> {
            term,
            mode: LeaderMode::Leading,
        };
        let following = LeaderState::<u16> {
            term,
            mode: LeaderMode::Following { leader: 7 },
        };
        let electing = LeaderState::<u16> {
            term,
            mode: LeaderMode::Electing { vote: Some(7) },
        };
        let no_leader = LeaderState::<u16> {
            term,
            mode: LeaderMode::NoLeader,
        };

        /* whichever voter answers, a leader exists; its lan address must not
         * leak into the observer's view */
        for state in [&leading, &following] {
            let view = GatewayView::from_voter_state(state, NOW);
            assert!(view.has_leader);
            assert_eq!(view.as_leader_state::<u16>(), leading);
        }

        for state in [&electing, &no_leader] {
            let view = GatewayView::from_voter_state(state, NOW);
            assert!(!view.has_leader);
            assert_eq!(view.as_leader_state::<u16>().mode, LeaderMode::Electing { vote: None });
            assert_eq!(view.as_leader_state::<u16>().term, term);
        }
    }

    #[test]
    fn gateway_view_expires() {
        let view = GatewayView::from_voter_state(
            &LeaderState::<u16> {
                term: ElectionTerm::from_term(1),
                mode: LeaderMode::Leading,
            },
            NOW,
        );
        assert!(view.is_fresh(NOW));
        assert!(!view.is_fresh(NOW + 1));
    }

    #[test]
    fn refreshed_failure_timestamp_does_not_reset_age() {
        /* discovery redials and rewrites the failure epoch every retry; the
         * peer must still count as expired */
        let failed = ConnectStatus::FailedToConnect { epoch_ms: NOW };
        assert!(peer(failed, Some(NOW - 2 * ms(HORIZON))).is_expired(NOW, HORIZON));
    }
}
