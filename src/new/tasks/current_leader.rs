use std::{
    collections::{BTreeSet, HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    iter,
    num::NonZeroU64,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::Mutex;

use crate::{
    new::{
        election::{decide_election, ElectionDecision, ElectionInput, PeerReachability, TimedPeerObservation},
        node_state::NodeState,
        subscribable_state::StateHandle,
    },
    protocol::messages::LeaderWithElectionInfo,
    state::{
        determinstic_state::DeterministicState,
        recoverable_state::{RecoverableStateAction, RecoverableStateDetails},
    },
    transport::traits::SyncIOAddress,
    utils::now_ms,
};

static GENERATION_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct CurrentLeaderStatus<A: SyncIOAddress> {
    local: A,
    state: Mutex<LeaderMode<A>>,
}

pub struct CurrentLeaderTask<A, D>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    state: Arc<NodeState<A, D>>,
    timing: CurrentLeaderTiming,
    state_handle: Mutex<StateHandle<D>>,
}

#[derive(Clone, Debug)]
pub struct CurrentLeaderTiming {
    pub election_interval: Duration,
    pub observation_stale_after: Duration,
}

impl Default for CurrentLeaderTiming {
    fn default() -> Self {
        Self {
            election_interval: Duration::from_secs(3),
            observation_stale_after: Duration::from_secs(15),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaderMode<A: SyncIOAddress> {
    NoLeader { term: u64 },
    Electing { term: u64 },
    Leading { term: u64, path: Vec<A> },
    Following { term: u64, leader: A, path: Vec<A>, via: A },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderStatusSnapshot<A: SyncIOAddress> {
    pub mode: LeaderMode<A>,
}

impl<A: SyncIOAddress> CurrentLeaderStatus<A> {
    pub fn new(local: A) -> Self {
        Self {
            local,
            state: Mutex::new(LeaderMode::NoLeader { term: 0 }),
        }
    }

    pub async fn snapshot(&self) -> LeaderStatusSnapshot<A> {
        LeaderStatusSnapshot {
            mode: self.state.lock().await.clone(),
        }
    }

    pub async fn current_term(&self) -> u64 {
        self.state.lock().await.term()
    }

    pub async fn leader(&self) -> Option<A> {
        self.state.lock().await.leader()
    }

    pub async fn path_to_leader(&self) -> Option<Vec<A>> {
        self.state.lock().await.path().cloned()
    }

    pub async fn begin_election(&self, observed_term: u64) -> u64 {
        let mut state = self.state.lock().await;
        let term = state.term().max(observed_term) + 1;
        *state = LeaderMode::Electing { term };
        term
    }

    pub async fn promote_self(&self, term: u64) {
        let mut state = self.state.lock().await;
        if term >= state.term() {
            *state = LeaderMode::Leading {
                term,
                path: vec![self.local],
            };
        }
    }

    pub async fn follow_remote(&self, leader: A, term: u64, path: Vec<A>, via: A) -> bool {
        if !valid_local_path(Some(leader), &path, self.local) {
            return false;
        }

        let mut state = self.state.lock().await;
        if term < state.term() {
            return false;
        }

        *state = LeaderMode::Following {
            term,
            leader,
            path,
            via,
        };
        true
    }

    pub async fn clear_if_leader(&self, leader: A) -> bool {
        let mut state = self.state.lock().await;
        if state.leader() != Some(leader) {
            return false;
        }

        let term = state.term();
        *state = LeaderMode::NoLeader { term };
        true
    }

    pub async fn clear_if_via(&self, via: A) -> bool {
        let mut state = self.state.lock().await;
        let LeaderMode::Following { via: current_via, .. } = *state else {
            return false;
        };
        if current_via != via {
            return false;
        }

        let term = state.term();
        *state = LeaderMode::NoLeader { term };
        true
    }

    pub async fn local_observation(
        &self,
        can_lead: bool,
        reachable_can_lead: Vec<A>,
        recover_details: RecoverableStateDetails,
    ) -> LeaderWithElectionInfo<A> {
        let state = self.state.lock().await;
        LeaderWithElectionInfo {
            observer: self.local,
            term: state.term(),
            leader: state.leader(),
            leader_path: state.path().cloned(),
            can_lead,
            reachable_can_lead,
            recover_details,
        }
    }
}

impl<A, D> CurrentLeaderTask<A, D>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    pub fn new(state: Arc<NodeState<A, D>>, timing: CurrentLeaderTiming) -> Self {
        let state_handle = Mutex::new(state.state.create_handle());
        Self {
            state,
            timing,
            state_handle,
        }
    }

    pub async fn run(self) {
        tracing::debug!(
            local = ?self.state.my_address,
            interval_ms = self.timing.election_interval.as_millis(),
            stale_after_ms = self.timing.observation_stale_after.as_millis(),
            "starting current leader task",
        );

        loop {
            self.tick().await;
            tokio::time::sleep(self.timing.election_interval).await;
        }
    }

    pub async fn tick(&self) {
        self.apply_election().await;
    }

    pub async fn apply_election(&self) {
        let now = now_ms();
        let stale_after_ms = self.timing.observation_stale_after.as_millis() as u64;

        let (known_can_lead, peer_observations, peer_reachability) = {
            let peers = self.state.peers.lock().await;
            let mut known_can_lead = peers
                .values()
                .filter_map(|peer| (peer.can_lead == Some(true)).then_some(peer.addr))
                .collect::<BTreeSet<_>>();

            if self.state.can_lead {
                known_can_lead.insert(self.state.my_address);
            }

            let peer_observations = peers
                .values()
                .filter_map(|peer| {
                    peer.leader_observation.clone().map(|observation| TimedPeerObservation {
                        observer: peer.addr,
                        last_activity_ms: peer.last_global_connectivity.map(NonZeroU64::get),
                        observation,
                    })
                })
                .collect::<Vec<_>>();

            let peer_reachability = peers
                .values()
                .map(|peer| {
                    (
                        peer.addr,
                        PeerReachability {
                            last_activity_ms: peer.last_global_connectivity.map(NonZeroU64::get),
                        },
                    )
                })
                .collect::<HashMap<_, _>>();

            (known_can_lead, peer_observations, peer_reachability)
        };

        let local_observation = local_leader_observation(&self.state, &self.state_handle).await;
        let election_term = self.state.leader_status.current_term().await;

        tracing::debug!(
            local = ?self.state.my_address,
            can_lead = self.state.can_lead,
            election_term,
            local_leader = ?local_observation.leader,
            local_leader_path = ?local_observation.leader_path,
            known_can_lead = ?known_can_lead,
            peer_observation_count = peer_observations.len(),
            peer_reachability_count = peer_reachability.len(),
            stale_after_ms,
            "applying leader election",
        );

        let decision = decide_election(ElectionInput {
            local_address: self.state.my_address,
            can_lead: self.state.can_lead,
            known_can_lead,
            local_observation,
            peer_observations,
            peer_reachability,
            election_term,
            now_ms: now,
            stale_after_ms,
        });
        tracing::debug!(
            local = ?self.state.my_address,
            ?decision,
            "leader election decision",
        );

        match decision {
            ElectionDecision::PromoteSelf { observed_term } => {
                if self.state.leader_status.leader().await == Some(self.state.my_address) {
                    tracing::debug!(
                        local = ?self.state.my_address,
                        observed_term,
                        "already leading; skipping self-promotion",
                    );
                    return;
                }

                let term = self.state.leader_status.begin_election(observed_term).await;
                let new_id = new_generation_id(self.state.my_address, term);
                tracing::debug!(
                    local = ?self.state.my_address,
                    observed_term,
                    term,
                    new_id,
                    "promoting self to leader",
                );
                self.state
                    .state
                    .update(iter::once(RecoverableStateAction::BumpGeneration { new_id }))
                    .await;
                self.state.leader_status.promote_self(term).await;
            }
            ElectionDecision::FollowRemote {
                leader,
                term,
                path,
                via,
            } => {
                let accepted = self
                    .state
                    .leader_status
                    .follow_remote(leader, term, path.clone(), via)
                    .await;
                tracing::debug!(
                    local = ?self.state.my_address,
                    ?leader,
                    term,
                    ?path,
                    ?via,
                    accepted,
                    "following remote leader",
                );
            }
            ElectionDecision::ClearRemoteLeader { leader } => {
                let cleared = self.state.leader_status.clear_if_leader(leader).await;
                tracing::debug!(
                    local = ?self.state.my_address,
                    ?leader,
                    cleared,
                    "clearing remote leader status",
                );
            }
            ElectionDecision::NoChange => {
                tracing::debug!(local = ?self.state.my_address, "leader election made no state change");
            }
        }
    }
}

pub async fn local_leader_observation<A, D>(
    state: &NodeState<A, D>,
    state_handle: &Mutex<StateHandle<D>>,
) -> LeaderWithElectionInfo<A>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    let recover_details = state_handle.lock().await.recover_details();
    let peers = state.peers.lock().await;
    let mut reachable_can_lead = peers
        .iter()
        .filter_map(|(_, peer)| (peer.is_connected && peer.can_lead == Some(true)).then_some(peer.addr))
        .collect::<Vec<_>>();
    if state.can_lead {
        reachable_can_lead.push(state.my_address);
    }
    drop(peers);

    state
        .leader_status
        .local_observation(state.can_lead, reachable_can_lead, recover_details)
        .await
}

impl<A: SyncIOAddress> LeaderMode<A> {
    pub fn term(&self) -> u64 {
        match self {
            LeaderMode::NoLeader { term }
            | LeaderMode::Electing { term }
            | LeaderMode::Leading { term, .. }
            | LeaderMode::Following { term, .. } => *term,
        }
    }

    pub fn leader(&self) -> Option<A> {
        match self {
            LeaderMode::Leading { path, .. } => path.first().copied(),
            LeaderMode::Following { leader, .. } => Some(*leader),
            LeaderMode::NoLeader { .. } | LeaderMode::Electing { .. } => None,
        }
    }

    pub fn path(&self) -> Option<&Vec<A>> {
        match self {
            LeaderMode::Leading { path, .. } | LeaderMode::Following { path, .. } => Some(path),
            LeaderMode::NoLeader { .. } | LeaderMode::Electing { .. } => None,
        }
    }
}

fn valid_local_path<A: SyncIOAddress>(leader: Option<A>, path: &[A], local: A) -> bool {
    let Some(leader) = leader else {
        return false;
    };
    if path.is_empty() || path[0] != leader || path.last().copied() != Some(local) {
        return false;
    }

    let mut seen = HashSet::new();
    path.iter().all(|item| seen.insert(*item))
}

fn new_generation_id<A: SyncIOAddress + Hash>(local: A, term: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    local.hash(&mut hasher);
    term.hash(&mut hasher);
    now_ms().hash(&mut hasher);
    GENERATION_COUNTER.fetch_add(1, Ordering::Relaxed).hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use sequenced_broadcast::SequencedBroadcastSettings;

    use super::*;
    use crate::{
        new::{
            node_state::{NodeState, PeerState},
            subscribable_state::SubscribableState,
        },
        state::recoverable_state::{RecoverableState, RecoverableStateDetails},
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
        })
    }

    fn task(state: Arc<NodeState<u64, TestState>>) -> CurrentLeaderTask<u64, TestState> {
        CurrentLeaderTask::new(state, CurrentLeaderTiming::default())
    }

    fn peer(addr: u64, can_lead: Option<bool>, is_connected: bool) -> PeerState<u64> {
        PeerState {
            addr,
            latency: None,
            can_lead,
            is_connected,
            last_global_connectivity: is_connected.then(|| NonZeroU64::new(now_ms()).unwrap()),
            leader_observation: None,
        }
    }

    fn leader_observation(observer: u64, term: u64, leader: u64, path: Vec<u64>) -> LeaderWithElectionInfo<u64> {
        LeaderWithElectionInfo {
            observer,
            term,
            leader: Some(leader),
            leader_path: Some(path),
            can_lead: true,
            reachable_can_lead: vec![observer],
            recover_details: RecoverableStateDetails::new(observer, 1),
        }
    }

    #[tokio::test]
    async fn starts_without_leader() {
        let status = CurrentLeaderStatus::new(1);

        assert_eq!(status.snapshot().await.mode, LeaderMode::NoLeader { term: 0 });
        assert_eq!(status.leader().await, None);
    }

    #[tokio::test]
    async fn promote_self_sets_leading_path() {
        let status = CurrentLeaderStatus::new(1);

        status.promote_self(2).await;

        assert_eq!(status.snapshot().await.mode, LeaderMode::Leading { term: 2, path: vec![1] });
    }

    #[tokio::test]
    async fn follow_remote_sets_following_path() {
        let status = CurrentLeaderStatus::new(3);

        assert!(status.follow_remote(1, 2, vec![1, 2, 3], 2).await);

        assert_eq!(
            status.snapshot().await.mode,
            LeaderMode::Following {
                term: 2,
                leader: 1,
                path: vec![1, 2, 3],
                via: 2,
            }
        );
    }

    #[tokio::test]
    async fn lower_term_follow_does_not_override_leading() {
        let status = CurrentLeaderStatus::new(1);
        status.promote_self(3).await;

        assert!(!status.follow_remote(2, 2, vec![2, 1], 2).await);

        assert_eq!(status.snapshot().await.mode, LeaderMode::Leading { term: 3, path: vec![1] });
    }

    #[tokio::test]
    async fn clear_if_via_removes_following_status() {
        let status = CurrentLeaderStatus::new(3);
        status.follow_remote(1, 2, vec![1, 2, 3], 2).await;

        assert!(status.clear_if_via(2).await);

        assert_eq!(status.snapshot().await.mode, LeaderMode::NoLeader { term: 2 });
    }

    #[tokio::test]
    async fn can_lead_node_promotes_when_no_reachable_leader() {
        let state = node_state(1, true, HashMap::new());
        let task = task(state.clone());

        task.apply_election().await;

        match state.leader_status.snapshot().await.mode {
            LeaderMode::Leading { path, .. } => assert_eq!(path, vec![1]),
            mode => panic!("expected leading mode, got {mode:?}"),
        }
    }

    #[tokio::test]
    async fn already_leading_node_does_not_advance_term() {
        let state = node_state(1, true, HashMap::new());
        let task = task(state.clone());

        task.apply_election().await;
        let first_term = state.leader_status.current_term().await;
        task.apply_election().await;

        assert_eq!(state.leader_status.current_term().await, first_term);
    }

    #[tokio::test]
    async fn non_leader_node_does_not_promote() {
        let state = node_state(1, false, HashMap::new());
        let task = task(state.clone());

        task.apply_election().await;

        assert_eq!(state.leader_status.snapshot().await.mode, LeaderMode::NoLeader { term: 0 });
    }

    #[tokio::test]
    async fn follows_reachable_remote_leader() {
        let mut peers = HashMap::new();
        let mut peer_two = peer(2, Some(true), true);
        peer_two.leader_observation = Some(leader_observation(2, 3, 2, vec![2]));
        peers.insert(2, peer_two);
        let state = node_state(1, true, peers);
        let task = task(state.clone());

        task.apply_election().await;

        assert_eq!(
            state.leader_status.snapshot().await.mode,
            LeaderMode::Following {
                term: 3,
                leader: 2,
                path: vec![2, 1],
                via: 2,
            }
        );
    }

    #[tokio::test]
    async fn clears_inaccessible_current_leader() {
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, Some(true), true));
        let state = node_state(1, false, peers);
        assert!(state.leader_status.follow_remote(2, 3, vec![2, 1], 2).await);

        state.leader_status.clear_if_via(2).await;

        assert_eq!(state.leader_status.snapshot().await.mode, LeaderMode::NoLeader { term: 3 });
    }

    #[tokio::test]
    async fn local_leader_observation_includes_connected_can_lead_peers() {
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, Some(true), true));
        peers.insert(3, peer(3, Some(true), false));
        let state = node_state(1, false, peers);
        let state_handle = Mutex::new(state.state.create_handle());

        let observation = local_leader_observation(&state, &state_handle).await;

        assert_eq!(observation.reachable_can_lead, vec![2]);
    }

    #[tokio::test]
    async fn local_leader_observation_includes_self_when_can_lead() {
        let state = node_state(1, true, HashMap::new());
        let state_handle = Mutex::new(state.state.create_handle());

        let observation = local_leader_observation(&state, &state_handle).await;

        assert_eq!(observation.reachable_can_lead, vec![1]);
    }
}
