use std::{
    collections::HashMap,
    num::NonZeroU64,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use tokio::sync::Mutex;

use crate::{
    new::{subscribable_state::SubscribableState, tasks::current_leader::CurrentLeaderStatus},
    protocol::messages::{LeaderWithElectionInfo, SharePeerDetails},
    state::determinstic_state::DeterministicState,
    transport::traits::SyncIOAddress,
    utils::now_ms,
};

pub struct NodeState<A: SyncIOAddress, D: DeterministicState> {
    pub my_address: A,
    pub can_lead: bool,
    pub peers: Mutex<HashMap<A, PeerState<A>>>,
    pub state: SubscribableState<D>,
    pub leader_status: Arc<CurrentLeaderStatus<A>>,
    pub election_term: AtomicU64,
}

pub struct PeerState<A: SyncIOAddress> {
    pub addr: A,
    pub latency: Option<NonZeroU64>,
    pub can_lead: Option<bool>,
    pub connect_status: ConnectStatus,
    pub last_global_connectivity: Option<NonZeroU64>,
    pub leader_observation: Option<LeaderWithElectionInfo<A>>,
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

    pub fn connected_at_ms(&self) -> Option<u64> {
        match self {
            Self::Connected { epoch_ms } => Some(*epoch_ms),
            Self::FailedToConnect { .. } | Self::NotConnected => None,
        }
    }

    pub fn failed_at_ms(&self) -> Option<u64> {
        match self {
            Self::FailedToConnect { epoch_ms } => Some(*epoch_ms),
            Self::Connected { .. } | Self::NotConnected => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PeerMergeResult {
    pub shared_count: usize,
    pub inserted: usize,
    pub updated: usize,
    pub skipped_local: usize,
}

impl<A: SyncIOAddress> PeerState<A> {
    pub(crate) fn empty(addr: A) -> Self {
        Self {
            addr,
            latency: None,
            can_lead: None,
            connect_status: ConnectStatus::NotConnected,
            last_global_connectivity: None,
            leader_observation: None,
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
    pub(crate) fn election_term(&self) -> u64 {
        self.election_term.load(Ordering::Acquire)
    }

    pub(crate) fn observe_election_term(&self, term: u64) -> u64 {
        self.election_term
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| (term > current).then_some(term))
            .map_or_else(|current| current, |_| term)
    }

    pub(crate) fn bump_election_term_after(&self, previous_term: u64) -> u64 {
        let next_term = previous_term.saturating_add(1);
        self.election_term
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| (next_term > current).then_some(next_term))
            .map_or_else(|current| current, |_| next_term)
    }

    pub(crate) async fn merge_peer_details(&self, shared_peers: Vec<SharePeerDetails<A>>) -> PeerMergeResult {
        let shared_count = shared_peers.len();
        let mut result = PeerMergeResult {
            shared_count,
            ..PeerMergeResult::default()
        };
        let mut peers = self.peers.lock().await;

        for shared in shared_peers {
            if shared.address == self.my_address {
                result.skipped_local += 1;
                continue;
            }

            let peer_state = peers.entry(shared.address).or_insert_with(|| {
                result.inserted += 1;
                PeerState::empty(shared.address)
            });
            result.updated += 1;

            if let Some(can_lead) = shared.can_be_leader {
                peer_state.can_lead = Some(can_lead);
            }

            peer_state.last_global_connectivity =
                merge_last_activity(peer_state.last_global_connectivity, shared.last_global_activity);
        }

        result
    }

    pub(crate) async fn known_peer_details(&self) -> Vec<SharePeerDetails<A>> {
        self.peers.lock().await.values().map(PeerState::share_details).collect()
    }

    pub(crate) async fn local_and_known_peer_details(&self) -> Vec<SharePeerDetails<A>> {
        let peers = self.peers.lock().await;
        let mut share = Vec::with_capacity(peers.len() + 1);
        share.push(SharePeerDetails {
            address: self.my_address,
            can_be_leader: Some(self.can_lead),
            last_global_activity: NonZeroU64::new(now_ms()),
        });
        share.extend(peers.values().map(PeerState::share_details));
        share
    }

    pub(crate) async fn record_leader_observation(&self, source: A, info: LeaderWithElectionInfo<A>) {
        self.observe_election_term(info.term);
        let mut peers = self.peers.lock().await;
        let peer_state = peers.entry(source).or_insert_with(|| PeerState::empty(source));
        peer_state.addr = source;
        peer_state.can_lead = Some(info.can_lead);
        peer_state.last_global_connectivity = NonZeroU64::new(now_ms());
        peer_state.leader_observation = Some(info);
    }

    pub(crate) async fn clear_peer_leader_observation(&self, peer: A) {
        let mut peers = self.peers.lock().await;
        peers
            .entry(peer)
            .or_insert_with(|| PeerState::empty(peer))
            .leader_observation = None;
    }

    pub(crate) async fn mark_peer_observed(&self, peer: A, latency: Option<NonZeroU64>) {
        let mut peers = self.peers.lock().await;
        let peer_state = peers.entry(peer).or_insert_with(|| PeerState::empty(peer));
        peer_state.latency = latency;
        peer_state.last_global_connectivity = NonZeroU64::new(now_ms());
    }

    pub(crate) async fn note_known_peer_activity(&self, peer: A) -> bool {
        let mut peers = self.peers.lock().await;
        let Some(peer_state) = peers.get_mut(&peer) else {
            return false;
        };
        peer_state.last_global_connectivity = NonZeroU64::new(now_ms());
        true
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, num::NonZeroU64, sync::Arc};

    use sequenced_broadcast::SequencedBroadcastSettings;
    use tokio::sync::Mutex;

    use super::*;
    use crate::{
        new::{subscribable_state::SubscribableState, tasks::current_leader::CurrentLeaderStatus},
        state::{
            determinstic_state::DeterministicState,
            recoverable_state::{RecoverableState, RecoverableStateDetails},
        },
    };

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestState(u64);

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestAction(u64);

    impl DeterministicState for TestState {
        type Action = TestAction;
        type AuthorityAction = TestAction;

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

    fn node_state(address: u64, can_lead: bool, peers: HashMap<u64, PeerState<u64>>) -> Arc<NodeState<u64, TestState>> {
        Arc::new(NodeState {
            my_address: address,
            can_lead,
            peers: Mutex::new(peers),
            state: SubscribableState::new(
                RecoverableState::new(address, TestState(1)),
                SequencedBroadcastSettings::default(),
            )
            .unwrap(),
            leader_status: Arc::new(CurrentLeaderStatus::new(address)),
            election_term: AtomicU64::new(0),
        })
    }

    fn peer(addr: u64, can_lead: Option<bool>, connect_status: ConnectStatus) -> PeerState<u64> {
        PeerState {
            addr,
            latency: None,
            can_lead,
            connect_status,
            last_global_connectivity: None,
            leader_observation: None,
        }
    }

    fn leader_observation(observer: u64, term: u64, leader: u64, path: Vec<u64>) -> LeaderWithElectionInfo<u64> {
        LeaderWithElectionInfo {
            observer,
            term,
            leader: Some(leader),
            leader_path: Some(path),
            vote: Some(leader),
            can_lead: true,
            reachable_can_lead: vec![observer],
            recover_details: RecoverableStateDetails::new(observer, 1),
        }
    }

    #[tokio::test]
    async fn observe_election_term_keeps_highest_seen_term() {
        let state = node_state(1, true, HashMap::new());

        assert_eq!(state.observe_election_term(2), 2);
        assert_eq!(state.observe_election_term(1), 2);
        assert_eq!(state.election_term(), 2);
    }

    #[tokio::test]
    async fn record_leader_observation_advances_election_term() {
        let state = node_state(1, true, HashMap::new());

        state
            .record_leader_observation(2, leader_observation(2, 4, 2, vec![2]))
            .await;

        assert_eq!(state.election_term(), 4);
    }

    #[tokio::test]
    async fn bump_election_term_after_advances_past_previous() {
        let state = node_state(1, true, HashMap::new());

        assert_eq!(state.bump_election_term_after(3), 4);
        assert_eq!(state.election_term(), 4);
    }

    #[tokio::test]
    async fn bump_election_term_after_preserves_higher_observed_term() {
        let state = node_state(1, true, HashMap::new());

        state.observe_election_term(7);

        assert_eq!(state.bump_election_term_after(3), 7);
        assert_eq!(state.election_term(), 7);
    }

    #[tokio::test]
    async fn merge_peer_details_inserts_new_peer() {
        let state = node_state(1, true, HashMap::new());

        let result = state
            .merge_peer_details(vec![SharePeerDetails {
                address: 2,
                can_be_leader: Some(true),
                last_global_activity: NonZeroU64::new(10),
            }])
            .await;

        assert_eq!(
            result,
            PeerMergeResult {
                shared_count: 1,
                inserted: 1,
                updated: 1,
                skipped_local: 0,
            }
        );
        let peers = state.peers.lock().await;
        let peer = peers.get(&2).unwrap();
        assert_eq!(peer.can_lead, Some(true));
        assert_eq!(peer.last_global_connectivity, NonZeroU64::new(10));
    }

    #[tokio::test]
    async fn merge_peer_details_skips_local_address() {
        let state = node_state(1, true, HashMap::new());

        let result = state
            .merge_peer_details(vec![SharePeerDetails {
                address: 1,
                can_be_leader: Some(false),
                last_global_activity: NonZeroU64::new(10),
            }])
            .await;

        assert_eq!(result.skipped_local, 1);
        assert!(state.peers.lock().await.is_empty());
    }

    #[tokio::test]
    async fn merge_peer_details_updates_can_lead_only_when_present() {
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, Some(true), ConnectStatus::NotConnected));
        let state = node_state(1, true, peers);

        state
            .merge_peer_details(vec![SharePeerDetails {
                address: 2,
                can_be_leader: None,
                last_global_activity: None,
            }])
            .await;
        assert_eq!(state.peers.lock().await.get(&2).unwrap().can_lead, Some(true));

        state
            .merge_peer_details(vec![SharePeerDetails {
                address: 2,
                can_be_leader: Some(false),
                last_global_activity: None,
            }])
            .await;
        assert_eq!(state.peers.lock().await.get(&2).unwrap().can_lead, Some(false));
    }

    #[tokio::test]
    async fn merge_peer_details_preserves_newest_global_activity() {
        let mut peers = HashMap::new();
        let mut peer = peer(2, None, ConnectStatus::NotConnected);
        peer.last_global_connectivity = NonZeroU64::new(20);
        peers.insert(2, peer);
        let state = node_state(1, true, peers);

        state
            .merge_peer_details(vec![SharePeerDetails {
                address: 2,
                can_be_leader: None,
                last_global_activity: NonZeroU64::new(10),
            }])
            .await;
        assert_eq!(state.peers.lock().await.get(&2).unwrap().last_global_connectivity, NonZeroU64::new(20));

        state
            .merge_peer_details(vec![SharePeerDetails {
                address: 2,
                can_be_leader: None,
                last_global_activity: NonZeroU64::new(30),
            }])
            .await;
        assert_eq!(state.peers.lock().await.get(&2).unwrap().last_global_connectivity, NonZeroU64::new(30));
    }

    #[tokio::test]
    async fn local_and_known_peer_details_includes_self() {
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, Some(false), ConnectStatus::NotConnected));
        let state = node_state(1, true, peers);

        let details = state.local_and_known_peer_details().await;

        assert_eq!(details.len(), 2);
        assert!(details.iter().any(|detail| {
            detail.address == 1 && detail.can_be_leader == Some(true) && detail.last_global_activity.is_some()
        }));
        assert!(details
            .iter()
            .any(|detail| detail.address == 2 && detail.can_be_leader == Some(false)));
    }

    #[tokio::test]
    async fn record_leader_observation_updates_peer_info() {
        let state = node_state(1, true, HashMap::new());

        state
            .record_leader_observation(2, leader_observation(2, 3, 2, vec![2]))
            .await;

        let peers = state.peers.lock().await;
        let peer = peers.get(&2).unwrap();
        assert_eq!(peer.addr, 2);
        assert_eq!(peer.can_lead, Some(true));
        assert!(peer.last_global_connectivity.is_some());
        assert_eq!(peer.leader_observation.as_ref().unwrap().leader, Some(2));
    }

    #[tokio::test]
    async fn clear_peer_leader_observation_does_not_change_connect_status() {
        let mut peers = HashMap::new();
        let mut peer = peer(2, Some(true), ConnectStatus::Connected { epoch_ms: 100 });
        peer.leader_observation = Some(leader_observation(2, 1, 2, vec![2]));
        peers.insert(2, peer);
        let state = node_state(1, false, peers);
        assert!(state.leader_status.follow_remote(2, vec![2, 1], 2).await);

        state.clear_peer_leader_observation(2).await;

        let peers = state.peers.lock().await;
        let peer = peers.get(&2).unwrap();
        assert_eq!(peer.connect_status, ConnectStatus::Connected { epoch_ms: 100 });
        assert!(peer.leader_observation.is_none());
        drop(peers);
        assert!(state.leader_status.path_to_leader().await.is_some());
    }

    #[tokio::test]
    async fn note_known_peer_activity_updates_existing_peer_only() {
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, None, ConnectStatus::NotConnected));
        let state = node_state(1, true, peers);

        assert!(state.note_known_peer_activity(2).await);
        assert!(!state.note_known_peer_activity(3).await);

        let peers = state.peers.lock().await;
        assert!(peers.get(&2).unwrap().last_global_connectivity.is_some());
        assert!(!peers.contains_key(&3));
    }

    #[tokio::test]
    async fn mark_peer_connected_inserts_missing_peer() {
        let state = node_state(1, true, HashMap::new());
        let before = now_ms();

        state.mark_peer_connected(2).await;
        let after = now_ms();

        let peers = state.peers.lock().await;
        let peer = peers.get(&2).unwrap();
        assert_eq!(peer.addr, 2);
        let ConnectStatus::Connected { epoch_ms } = peer.connect_status else {
            panic!("expected connected status, got {:?}", peer.connect_status);
        };
        assert!((before..=after).contains(&epoch_ms));
        assert_eq!(peer.can_lead, None);
    }

    #[tokio::test]
    async fn mark_peer_not_connected_sets_not_connected() {
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, None, ConnectStatus::Connected { epoch_ms: 100 }));
        let state = node_state(1, true, peers);

        state.mark_peer_not_connected(2).await;

        assert_eq!(state.peers.lock().await.get(&2).unwrap().connect_status, ConnectStatus::NotConnected);
    }

    #[tokio::test]
    async fn mark_peer_failed_to_connect_records_transition_time() {
        let state = node_state(1, true, HashMap::new());
        let before = now_ms();

        state.mark_peer_failed_to_connect(2).await;
        let after = now_ms();

        let peers = state.peers.lock().await;
        let ConnectStatus::FailedToConnect { epoch_ms } = peers.get(&2).unwrap().connect_status else {
            panic!("expected failed-to-connect status");
        };
        assert!((before..=after).contains(&epoch_ms));
    }

    #[tokio::test]
    async fn failed_to_connect_timestamp_updates_on_new_failure_transition() {
        let state = node_state(1, true, HashMap::new());

        state.mark_peer_failed_to_connect(2).await;
        let first = state
            .peers
            .lock()
            .await
            .get(&2)
            .unwrap()
            .connect_status
            .failed_at_ms()
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        state.mark_peer_failed_to_connect(2).await;
        let second = state
            .peers
            .lock()
            .await
            .get(&2)
            .unwrap()
            .connect_status
            .failed_at_ms()
            .unwrap();

        assert!(second >= first);
    }

    #[test]
    fn connect_status_helpers_identify_connected_and_failed_times() {
        let connected = ConnectStatus::Connected { epoch_ms: 10 };
        let failed = ConnectStatus::FailedToConnect { epoch_ms: 20 };
        let not_connected = ConnectStatus::NotConnected;

        assert!(connected.is_connected());
        assert_eq!(connected.connected_at_ms(), Some(10));
        assert_eq!(connected.failed_at_ms(), None);
        assert!(!failed.is_connected());
        assert_eq!(failed.connected_at_ms(), None);
        assert_eq!(failed.failed_at_ms(), Some(20));
        assert!(!not_connected.is_connected());
        assert_eq!(not_connected.connected_at_ms(), None);
        assert_eq!(not_connected.failed_at_ms(), None);
    }
}
