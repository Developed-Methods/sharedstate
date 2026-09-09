use std::{
    collections::HashMap,
    num::NonZeroU64,
    sync::{
        RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::{
    sync::{Mutex, Notify},
    time::Instant,
};

use crate::{
    cluster::election::LeadershipPermit,
    protocol::messages::{LeaderInfo, LeaderMode, LeaderState, LeadershipEpoch, SharePeerDetails},
    state::{deterministic_state::DeterministicState, subscribable_state::SubscribableState},
    transport::traits::SyncIOAddress,
    utils::now_ms,
};

pub struct NodeState<A: SyncIOAddress, D: DeterministicState> {
    pub my_address: A,
    pub can_lead: bool,
    pub peers: Mutex<HashMap<A, PeerState<A>>>,
    pub state: SubscribableState<D>,
    pub election: RwLock<ElectionStatus<A>>,
    pub leadership_changed: Notify,
    pub eligible: AtomicBool,
    pub synced_epoch: RwLock<Option<LeadershipEpoch>>,
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

#[derive(Clone, Debug)]
pub struct ElectionStatus<A: SyncIOAddress> {
    pub owner: Option<A>,
    pub epoch: LeadershipEpoch,
    pub ready: bool,
    pub valid_until: Option<Instant>,
    pub permit: Option<LeadershipPermit>,
    pub error: Option<String>,
}

impl<A: SyncIOAddress> Default for ElectionStatus<A> {
    fn default() -> Self {
        Self {
            owner: None,
            epoch: LeadershipEpoch::default(),
            ready: false,
            valid_until: None,
            permit: None,
            error: None,
        }
    }
}

impl<A: SyncIOAddress> ElectionStatus<A> {
    pub fn valid(&self) -> bool {
        self.valid_until.is_some_and(|deadline| Instant::now() < deadline)
            && self.permit.as_ref().is_none_or(LeadershipPermit::valid)
    }

    fn permits(&self, owner: A, epoch: LeadershipEpoch, ready: bool) -> bool {
        self.owner == Some(owner) && self.epoch == epoch && (!ready || self.ready) && self.valid()
    }
}

impl<A: SyncIOAddress, D: DeterministicState> NodeState<A, D> {
    pub fn election_status(&self) -> ElectionStatus<A> {
        self.election.read().unwrap().clone()
    }

    pub fn current_leader(&self) -> LeaderState<A> {
        let status = self.election.read().unwrap();
        let mode = if !status.valid() {
            LeaderMode::NoLeader
        } else if !status.ready {
            LeaderMode::Electing
        } else if status.owner == Some(self.my_address) && status.permit.is_some() {
            LeaderMode::Leading
        } else if let Some(leader) = status.owner {
            LeaderMode::Following { leader }
        } else {
            LeaderMode::NoLeader
        };
        LeaderState {
            epoch: status.epoch,
            mode,
        }
    }

    pub fn valid_authority(&self, epoch: LeadershipEpoch) -> bool {
        let status = self.election.read().unwrap();
        status.permit.is_some() && status.permits(self.my_address, epoch, true)
    }

    pub fn valid_replication(&self, owner: A, epoch: LeadershipEpoch) -> bool {
        self.election.read().unwrap().permits(owner, epoch, true)
    }

    pub fn replication_epoch(&self) -> Option<LeadershipEpoch> {
        let status = self.election.read().unwrap();
        if !status.ready || !status.valid() {
            return None;
        }
        let synced = self.synced_epoch.read().unwrap();
        (status.permit.is_some() || *synced == Some(status.epoch)).then_some(status.epoch)
    }

    pub fn mark_synchronized(&self, epoch: LeadershipEpoch) {
        let status = self.election.read().unwrap();
        if status.ready && status.valid() && status.epoch == epoch {
            *self.synced_epoch.write().unwrap() = Some(epoch);
            self.eligible.store(true, Ordering::Release);
        }
    }

    pub fn set_election_status(&self, status: ElectionStatus<A>) {
        let mut current = self.election.write().unwrap();
        let changed = current.owner != status.owner || current.epoch != status.epoch || current.ready != status.ready;
        if changed {
            *self.synced_epoch.write().unwrap() = None;
        }
        *current = status;
        drop(current);
        if changed {
            self.leadership_changed.notify_waiters();
        }
    }

    pub fn revoke_authority(&self, error: Option<String>) {
        let mut status = self.election.write().unwrap();
        if let Some(permit) = &status.permit {
            permit.revoke();
        }
        status.ready = false;
        status.valid_until = None;
        status.error = error;
        drop(status);
        self.leadership_changed.notify_waiters();
    }

    pub async fn update_authoritative(
        &self,
        epoch: LeadershipEpoch,
        action: crate::state::recoverable_state::RecoverableStateAction<D::AuthorityAction>,
    ) -> bool {
        self.state
            .update_guarded(std::iter::once(action), || {
                let status = self.election.read().unwrap();
                (status.permit.is_some() && status.permits(self.my_address, epoch, true)).then_some(status)
            })
            .await
    }

    pub async fn update_preparing(
        &self,
        epoch: LeadershipEpoch,
        action: crate::state::recoverable_state::RecoverableStateAction<D::AuthorityAction>,
    ) -> bool {
        self.state
            .update_guarded(std::iter::once(action), || {
                let status = self.election.read().unwrap();
                (status.permit.is_some() && status.permits(self.my_address, epoch, false)).then_some(status)
            })
            .await
    }

    pub async fn update_replica(
        &self,
        owner: A,
        epoch: LeadershipEpoch,
        action: crate::state::recoverable_state::RecoverableStateAction<D::AuthorityAction>,
    ) -> bool {
        self.state
            .update_guarded(std::iter::once(action), || {
                let status = self.election.read().unwrap();
                status.permits(owner, epoch, true).then_some(status)
            })
            .await
    }

    pub async fn reset_replica(
        &self,
        owner: A,
        epoch: LeadershipEpoch,
        fresh: crate::state::recoverable_state::RecoverableState<D>,
    ) -> bool {
        self.state
            .reset_guarded(fresh, || {
                let status = self.election.read().unwrap();
                status.permits(owner, epoch, true).then_some(status)
            })
            .await
    }

    pub async fn reset_preparing(
        &self,
        epoch: LeadershipEpoch,
        fresh: crate::state::recoverable_state::RecoverableState<D>,
    ) -> bool {
        self.state
            .reset_guarded(fresh, || {
                let status = self.election.read().unwrap();
                (status.permit.is_some() && status.permits(self.my_address, epoch, false)).then_some(status)
            })
            .await
    }
}
