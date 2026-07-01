use std::{
    collections::{BTreeSet, HashMap},
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
        election::{
            can_lead_majority, choose_vote, conflicting_published_leaders, find_published_leader,
            leader_offline_vote_count, peer_is_fresh, tally_votes, ElectionInput, PeerReachability,
            TimedPeerObservation,
        },
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

pub struct CurrentLeaderTask<A, D>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    state: Arc<NodeState<A, D>>,
    timing: CurrentLeaderTiming,
    state_handle: Mutex<StateHandle<D>>,
    last_considered_term: u64,
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
    NoLeader,
    Electing { vote: Option<A> },
    Leading { path: Vec<A> },
    Following { leader: A, path: Vec<A>, via: A },
}

impl<A: SyncIOAddress> LeaderMode<A> {
    pub fn leader(&self) -> Option<A> {
        match self {
            Self::NoLeader | Self::Electing { .. } => None,
            Self::Leading { path } => path.first().copied(),
            Self::Following { leader, .. } => Some(*leader),
        }
    }

    pub fn path(&self) -> Option<&Vec<A>> {
        match self {
            Self::NoLeader | Self::Electing { .. } => None,
            Self::Leading { path } | Self::Following { path, .. } => Some(path),
        }
    }

    pub fn vote(&self, local: A) -> Option<A> {
        match self {
            Self::NoLeader => None,
            Self::Electing { vote } => *vote,
            Self::Leading { .. } => Some(local),
            Self::Following { leader, .. } => Some(*leader),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderStatusSnapshot<A: SyncIOAddress> {
    pub mode: LeaderMode<A>,
}

pub struct CurrentLeaderStatus<A: SyncIOAddress> {
    local: A,
    state: Mutex<LeaderMode<A>>,
}

impl<A: SyncIOAddress> CurrentLeaderStatus<A> {
    pub fn new(local: A) -> Self {
        Self {
            local,
            state: Mutex::new(LeaderMode::NoLeader),
        }
    }

    pub async fn snapshot(&self) -> LeaderStatusSnapshot<A> {
        LeaderStatusSnapshot {
            mode: self.state.lock().await.clone(),
        }
    }

    pub async fn leader(&self) -> Option<A> {
        self.state.lock().await.leader()
    }

    pub async fn path_to_leader(&self) -> Option<Vec<A>> {
        self.state.lock().await.path().cloned()
    }

    pub async fn vote(&self) -> Option<A> {
        self.state.lock().await.vote(self.local)
    }

    pub async fn no_leader(&self) {
        *self.state.lock().await = LeaderMode::NoLeader;
    }

    pub async fn electing(&self, vote: Option<A>) {
        *self.state.lock().await = LeaderMode::Electing { vote };
    }

    pub async fn set_vote(&self, vote: Option<A>) {
        let mut state = self.state.lock().await;
        match &mut *state {
            LeaderMode::Electing { vote: current_vote } => {
                *current_vote = vote;
            }
            _ => {
                *state = LeaderMode::Electing { vote };
            }
        }
    }

    pub async fn promote_self(&self) {
        *self.state.lock().await = LeaderMode::Leading { path: vec![self.local] };
    }

    pub async fn follow_remote(&self, leader: A, path: Vec<A>, via: A) -> bool {
        if path.is_empty() || path.first().copied() != Some(leader) || path.last().copied() != Some(self.local) {
            return false;
        }
        if !path.contains(&via) {
            return false;
        }

        *self.state.lock().await = LeaderMode::Following { leader, path, via };
        true
    }

    pub async fn local_observation(
        &self,
        term: u64,
        can_lead: bool,
        reachable_can_lead: Vec<A>,
        recover_details: RecoverableStateDetails,
    ) -> LeaderWithElectionInfo<A> {
        let state = self.state.lock().await;
        LeaderWithElectionInfo {
            observer: self.local,
            term,
            leader: state.leader(),
            leader_path: state.path().cloned(),
            vote: state.vote(self.local),
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
            last_considered_term: 0,
        }
    }

    pub async fn run(mut self) {
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

    pub async fn tick(&mut self) {
        self.apply_current_leader_tick().await;
    }

    pub async fn apply_current_leader_tick(&mut self) {
        let mut term = self.state.election_term();
        if term == 0 {
            term = self.state.bump_election_term_after(0);
        }

        let term_changed = self.last_considered_term != term;
        if term_changed {
            tracing::debug!(
                local = ?self.state.my_address,
                previous_term = self.last_considered_term,
                term,
                "current leader task considering new election term",
            );
            self.last_considered_term = term;
            self.state.leader_status.no_leader().await;
        }

        let snapshot = self.state.leader_status.snapshot().await;
        let input = self.build_election_input(term).await;

        if self.trigger_new_election_on_conflicting_leaders(term, &input).await {
            return;
        }

        if !term_changed {
            match snapshot.mode {
                LeaderMode::Following { leader, .. } => {
                    self.verify_following_leader(term, leader, input).await;
                    return;
                }
                LeaderMode::Leading { .. } => {
                    tracing::debug!(
                        local = ?self.state.my_address,
                        term,
                        "already leading for current election term",
                    );
                    return;
                }
                LeaderMode::NoLeader | LeaderMode::Electing { .. } => {}
            }
        }

        self.evaluate_term(term, input).await;
    }

    async fn trigger_new_election_on_conflicting_leaders(&mut self, term: u64, input: &ElectionInput<A>) -> bool {
        let leaders = conflicting_published_leaders(input);
        if leaders.len() <= 1 {
            return false;
        }

        let new_term = self.state.bump_election_term_after(term);
        self.last_considered_term = new_term;
        self.state.leader_status.no_leader().await;
        let input = self.build_election_input(new_term).await;
        let vote = self.local_vote(&input);
        self.state.leader_status.electing(vote).await;

        tracing::debug!(
            local = ?self.state.my_address,
            old_term = term,
            new_term,
            leaders = ?leaders,
            vote = ?vote,
            "bumped election term because multiple leaders were published",
        );
        true
    }

    async fn evaluate_term(&mut self, term: u64, input: ElectionInput<A>) {
        if let Some(published) = find_published_leader(&input) {
            tracing::debug!(
                local = ?self.state.my_address,
                term,
                leader = ?published.leader,
                path = ?published.path,
                via = ?published.via,
                "found published leader for election term",
            );

            if published.leader == self.state.my_address {
                if self.state.can_lead {
                    self.promote_for_term(term).await;
                }
            } else if let Some(via) = published.via {
                let followed = self
                    .state
                    .leader_status
                    .follow_remote(published.leader, published.path, via)
                    .await;
                tracing::debug!(
                    local = ?self.state.my_address,
                    term,
                    leader = ?published.leader,
                    followed,
                    "applied published remote leader",
                );
            }
            return;
        }

        let vote = self.local_vote(&input);
        self.state.leader_status.electing(vote).await;
        let input = self.build_election_input(term).await;
        let tally = tally_votes(&input);

        tracing::debug!(
            local = ?self.state.my_address,
            term,
            vote = ?vote,
            tally = ?tally,
            "evaluated election votes",
        );

        let Some(tally) = tally else {
            return;
        };
        if tally.votes < tally.majority {
            return;
        }

        if tally.candidate == self.state.my_address {
            if self.state.can_lead {
                self.promote_for_term(term).await;
            }
        } else {
            self.state.leader_status.set_vote(Some(tally.candidate)).await;
        }
    }

    async fn verify_following_leader(&mut self, term: u64, leader: A, input: ElectionInput<A>) {
        if peer_is_fresh(leader, self.state.my_address, &input.peer_reachability, input.now_ms, input.stale_after_ms) {
            tracing::debug!(
                local = ?self.state.my_address,
                term,
                leader = ?leader,
                "following leader is still locally reachable",
            );
            return;
        }

        if !self.state.can_lead {
            tracing::debug!(
                local = ?self.state.my_address,
                term,
                leader = ?leader,
                "following leader is locally unreachable but node cannot decide election",
            );
            return;
        }

        let offline_votes = leader_offline_vote_count(&input, leader);
        let majority = can_lead_majority(&input.known_can_lead);
        tracing::debug!(
            local = ?self.state.my_address,
            term,
            leader = ?leader,
            offline_votes,
            majority,
            "verified following leader accessibility",
        );

        if offline_votes < majority {
            return;
        }

        let new_term = self.state.bump_election_term_after(term);
        self.last_considered_term = new_term;
        self.state.leader_status.no_leader().await;
        let input = self.build_election_input(new_term).await;
        let vote = self.local_vote(&input);
        self.state.leader_status.electing(vote).await;
        tracing::debug!(
            local = ?self.state.my_address,
            old_term = term,
            new_term,
            leader = ?leader,
            vote = ?vote,
            "bumped election term because current leader is inaccessible",
        );
    }

    async fn promote_for_term(&self, term: u64) {
        let snapshot = self.state.leader_status.snapshot().await;
        if matches!(snapshot.mode, LeaderMode::Leading { path } if path.as_slice() == [self.state.my_address]) {
            return;
        }

        self.state.observe_election_term(term);
        let new_id = new_generation_id(self.state.my_address, term);
        self.state
            .state
            .update(iter::once(RecoverableStateAction::BumpGeneration { new_id }))
            .await;
        self.state.leader_status.promote_self().await;
        tracing::debug!(
            local = ?self.state.my_address,
            term,
            new_generation_id = new_id,
            "promoted local node as leader",
        );
    }

    fn local_vote(&self, input: &ElectionInput<A>) -> Option<A> {
        self.state.can_lead.then(|| choose_vote(input)).flatten()
    }

    async fn build_election_input(&self, term: u64) -> ElectionInput<A> {
        let now = now_ms();
        let stale_after_ms = duration_ms(self.timing.observation_stale_after);
        let local_observation = local_leader_observation(&self.state, &self.state_handle).await;
        let peers = self.state.peers.lock().await;

        let mut known_can_lead = BTreeSet::new();
        if self.state.can_lead {
            known_can_lead.insert(self.state.my_address);
        }

        let mut peer_observations = Vec::new();
        let mut peer_reachability = HashMap::new();
        for peer in peers.values() {
            if peer.can_lead == Some(true) {
                known_can_lead.insert(peer.addr);
            }

            peer_reachability.insert(
                peer.addr,
                PeerReachability {
                    last_activity_ms: peer.last_global_connectivity.map(NonZeroU64::get),
                },
            );

            if let Some(observation) = &peer.leader_observation {
                peer_observations.push(TimedPeerObservation {
                    observer: peer.addr,
                    last_activity_ms: peer.last_global_connectivity.map(NonZeroU64::get),
                    observation: observation.clone(),
                });
            }
        }

        ElectionInput {
            local_address: self.state.my_address,
            can_lead: self.state.can_lead,
            known_can_lead,
            local_observation,
            peer_observations,
            peer_reachability,
            election_term: term,
            now_ms: now,
            stale_after_ms,
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
        .values()
        .filter_map(|peer| (peer.connect_status.is_connected() && peer.can_lead == Some(true)).then_some(peer.addr))
        .collect::<Vec<_>>();
    if state.can_lead {
        reachable_can_lead.push(state.my_address);
    }
    reachable_can_lead.sort();
    reachable_can_lead.dedup();
    drop(peers);

    state
        .leader_status
        .local_observation(state.election_term(), state.can_lead, reachable_can_lead, recover_details)
        .await
}

fn new_generation_id<A: SyncIOAddress + Hash>(local: A, term: u64) -> u64 {
    let counter = GENERATION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut hasher = DefaultHasher::new();
    local.hash(&mut hasher);
    term.hash(&mut hasher);
    counter.hash(&mut hasher);
    hasher.finish()
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        num::NonZeroU64,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::Duration,
    };

    use sequenced_broadcast::SequencedBroadcastSettings;

    use super::*;
    use crate::{
        new::{
            node_state::{ConnectStatus, PeerState},
            subscribable_state::SubscribableState,
        },
        state::recoverable_state::RecoverableState,
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

    fn node_state(
        address: u64,
        can_lead: bool,
        peers: HashMap<u64, PeerState<u64>>,
        term: u64,
    ) -> Arc<NodeState<u64, TestState>> {
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
            election_term: AtomicU64::new(term),
        })
    }

    fn timing() -> CurrentLeaderTiming {
        CurrentLeaderTiming {
            election_interval: Duration::from_millis(10),
            observation_stale_after: Duration::from_millis(1_000),
        }
    }

    fn peer(
        addr: u64,
        can_lead: Option<bool>,
        connect_status: ConnectStatus,
        last_seen: Option<u64>,
    ) -> PeerState<u64> {
        PeerState {
            addr,
            latency: None,
            can_lead,
            connect_status,
            last_global_connectivity: last_seen.and_then(NonZeroU64::new),
            leader_observation: None,
        }
    }

    fn observed_peer(
        addr: u64,
        can_lead: bool,
        last_seen: u64,
        observation: LeaderWithElectionInfo<u64>,
    ) -> PeerState<u64> {
        let mut peer = peer(addr, Some(can_lead), ConnectStatus::Connected { epoch_ms: last_seen }, Some(last_seen));
        peer.leader_observation = Some(observation);
        peer
    }

    fn observation(
        observer: u64,
        term: u64,
        leader: Option<u64>,
        leader_path: Option<Vec<u64>>,
        vote: Option<u64>,
        can_lead: bool,
        reachable_can_lead: Vec<u64>,
    ) -> LeaderWithElectionInfo<u64> {
        LeaderWithElectionInfo {
            observer,
            term,
            leader,
            leader_path,
            vote,
            can_lead,
            reachable_can_lead,
            recover_details: RecoverableStateDetails::new(observer, 1),
        }
    }

    #[tokio::test]
    async fn starts_without_leader() {
        let status = CurrentLeaderStatus::new(1);

        assert_eq!(status.snapshot().await.mode, LeaderMode::NoLeader);
        assert_eq!(status.leader().await, None);
        assert_eq!(status.vote().await, None);
    }

    #[tokio::test]
    async fn promote_self_sets_leading_path() {
        let status = CurrentLeaderStatus::new(1);

        status.promote_self().await;

        assert_eq!(status.snapshot().await.mode, LeaderMode::Leading { path: vec![1] });
        assert_eq!(status.leader().await, Some(1));
        assert_eq!(status.vote().await, Some(1));
    }

    #[tokio::test]
    async fn follow_remote_sets_following_path() {
        let status = CurrentLeaderStatus::new(1);

        assert!(status.follow_remote(2, vec![2, 1], 2).await);

        assert_eq!(
            status.snapshot().await.mode,
            LeaderMode::Following {
                leader: 2,
                path: vec![2, 1],
                via: 2,
            }
        );
        assert_eq!(status.vote().await, Some(2));
    }

    #[tokio::test]
    async fn local_observation_publishes_current_node_state_term_and_vote() {
        let state = node_state(1, true, HashMap::new(), 5);
        state.leader_status.electing(Some(2)).await;
        let handle = Mutex::new(state.state.create_handle());

        let observation = local_leader_observation(&state, &handle).await;

        assert_eq!(observation.observer, 1);
        assert_eq!(observation.term, 5);
        assert_eq!(observation.leader, None);
        assert_eq!(observation.vote, Some(2));
    }

    #[tokio::test]
    async fn new_term_without_published_leader_enters_election_with_vote() {
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, Some(true), ConnectStatus::NotConnected, None));
        peers.insert(3, peer(3, Some(true), ConnectStatus::NotConnected, None));
        let state = node_state(1, true, peers, 0);
        let mut task = CurrentLeaderTask::new(state.clone(), timing());

        task.tick().await;

        assert_eq!(state.election_term(), 1);
        assert_eq!(state.leader_status.snapshot().await.mode, LeaderMode::Electing { vote: Some(1) });
    }

    #[tokio::test]
    async fn non_can_lead_node_does_not_vote_without_published_leader() {
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, Some(true), ConnectStatus::NotConnected, None));
        let state = node_state(1, false, peers, 0);
        let mut task = CurrentLeaderTask::new(state.clone(), timing());

        task.tick().await;

        assert_eq!(state.leader_status.snapshot().await.mode, LeaderMode::Electing { vote: None });
    }

    #[tokio::test]
    async fn published_remote_leader_is_followed() {
        let now = now_ms();
        let mut peers = HashMap::new();
        peers.insert(2, observed_peer(2, true, now, observation(2, 2, Some(2), Some(vec![2]), Some(2), true, vec![2])));
        let state = node_state(1, true, peers, 2);
        let mut task = CurrentLeaderTask::new(state.clone(), timing());

        task.tick().await;

        assert_eq!(
            state.leader_status.snapshot().await.mode,
            LeaderMode::Following {
                leader: 2,
                path: vec![2, 1],
                via: 2,
            }
        );
    }

    #[tokio::test]
    async fn published_local_leader_promotes_self() {
        let now = now_ms();
        let mut peers = HashMap::new();
        peers.insert(
            2,
            observed_peer(2, true, now, observation(2, 2, Some(1), Some(vec![1, 2]), Some(1), true, vec![1, 2])),
        );
        let state = node_state(1, true, peers, 2);
        let mut task = CurrentLeaderTask::new(state.clone(), timing());

        task.tick().await;

        assert_eq!(state.leader_status.snapshot().await.mode, LeaderMode::Leading { path: vec![1] });
    }

    #[tokio::test]
    async fn majority_votes_for_self_promotes_self() {
        let now = now_ms();
        let mut peers = HashMap::new();
        peers.insert(2, observed_peer(2, true, now, observation(2, 1, None, None, Some(1), true, vec![1, 2])));
        peers.insert(3, observed_peer(3, true, now, observation(3, 1, None, None, Some(1), true, vec![1, 3])));
        let state = node_state(1, true, peers, 1);
        let mut task = CurrentLeaderTask::new(state.clone(), timing());

        task.tick().await;

        assert_eq!(state.leader_status.snapshot().await.mode, LeaderMode::Leading { path: vec![1] });
    }

    #[tokio::test]
    async fn majority_votes_for_remote_waits_for_remote_publish() {
        let now = now_ms();
        let mut peers = HashMap::new();
        peers.insert(2, observed_peer(2, true, now, observation(2, 1, None, None, Some(2), true, vec![2])));
        peers.insert(3, observed_peer(3, true, now, observation(3, 1, None, None, Some(2), true, vec![2, 3])));
        let state = node_state(1, true, peers, 1);
        let mut task = CurrentLeaderTask::new(state.clone(), timing());

        task.tick().await;

        assert_eq!(state.leader_status.snapshot().await.mode, LeaderMode::Electing { vote: Some(2) });
    }

    #[tokio::test]
    async fn following_same_term_accessible_leader_does_not_recompute_vote() {
        let now = now_ms();
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, Some(true), ConnectStatus::Connected { epoch_ms: now }, Some(now)));
        let state = node_state(1, true, peers, 1);
        assert!(state.leader_status.follow_remote(2, vec![2, 1], 2).await);
        let mut task = CurrentLeaderTask::new(state.clone(), timing());
        task.last_considered_term = 1;

        task.tick().await;

        assert_eq!(
            state.leader_status.snapshot().await.mode,
            LeaderMode::Following {
                leader: 2,
                path: vec![2, 1],
                via: 2,
            }
        );
    }

    #[tokio::test]
    async fn following_same_term_with_two_published_leaders_bumps_term_and_elects() {
        let now = now_ms();
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, Some(true), ConnectStatus::Connected { epoch_ms: now }, Some(now)));
        peers.insert(3, observed_peer(3, true, now, observation(3, 1, Some(3), Some(vec![3]), Some(3), true, vec![3])));
        let state = node_state(1, true, peers, 1);
        assert!(state.leader_status.follow_remote(2, vec![2, 1], 2).await);
        let mut task = CurrentLeaderTask::new(state.clone(), timing());
        task.last_considered_term = 1;

        task.tick().await;

        assert_eq!(state.election_term(), 2);
        assert!(matches!(state.leader_status.snapshot().await.mode, LeaderMode::Electing { .. }));
    }

    #[tokio::test]
    async fn following_same_term_inaccessible_without_majority_offline_keeps_following() {
        let now = now_ms();
        let stale = now - 2_000;
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, Some(true), ConnectStatus::NotConnected, Some(stale)));
        peers.insert(3, peer(3, Some(true), ConnectStatus::NotConnected, None));
        let state = node_state(1, true, peers, 1);
        assert!(state.leader_status.follow_remote(2, vec![2, 1], 2).await);
        let mut task = CurrentLeaderTask::new(state.clone(), timing());
        task.last_considered_term = 1;

        task.tick().await;

        assert_eq!(state.election_term(), 1);
        assert!(matches!(state.leader_status.snapshot().await.mode, LeaderMode::Following { leader: 2, .. }));
    }

    #[tokio::test]
    async fn following_same_term_inaccessible_with_majority_offline_bumps_term_and_elects() {
        let now = now_ms();
        let stale = now - 2_000;
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, Some(true), ConnectStatus::NotConnected, Some(stale)));
        peers.insert(3, observed_peer(3, true, now, observation(3, 1, None, None, Some(3), true, vec![3])));
        let state = node_state(1, true, peers, 1);
        assert!(state.leader_status.follow_remote(2, vec![2, 1], 2).await);
        let mut task = CurrentLeaderTask::new(state.clone(), timing());
        task.last_considered_term = 1;

        task.tick().await;

        assert_eq!(state.election_term(), 2);
        assert!(matches!(state.leader_status.snapshot().await.mode, LeaderMode::Electing { .. }));
    }

    #[tokio::test]
    async fn leading_same_term_does_not_advance_generation() {
        let state = node_state(1, true, HashMap::new(), 1);
        state.leader_status.promote_self().await;
        let mut task = CurrentLeaderTask::new(state.clone(), timing());
        task.last_considered_term = 1;
        let mut handle = state.state.create_handle();
        let before = handle.recover_details();

        task.tick().await;

        assert_eq!(state.election_term(), 1);
        assert_eq!(handle.recover_details(), before);
        assert_eq!(state.leader_status.snapshot().await.mode, LeaderMode::Leading { path: vec![1] });
    }

    #[tokio::test]
    async fn local_leader_observation_includes_connected_can_lead_peers() {
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, Some(true), ConnectStatus::Connected { epoch_ms: 100 }, Some(100)));
        peers.insert(3, peer(3, Some(true), ConnectStatus::NotConnected, None));
        peers.insert(4, peer(4, Some(true), ConnectStatus::FailedToConnect { epoch_ms: 100 }, None));
        peers.insert(5, peer(5, Some(false), ConnectStatus::Connected { epoch_ms: 100 }, Some(100)));
        let state = node_state(1, true, peers, 1);
        let handle = Mutex::new(state.state.create_handle());

        let observation = local_leader_observation(&state, &handle).await;

        assert_eq!(observation.reachable_can_lead, vec![1, 2]);
    }

    #[test]
    fn leader_mode_vote_uses_role_state() {
        assert_eq!(LeaderMode::<u64>::NoLeader.vote(1), None);
        assert_eq!(LeaderMode::Electing { vote: Some(2) }.vote(1), Some(2));
        assert_eq!(LeaderMode::Leading { path: vec![1] }.vote(1), Some(1));
        assert_eq!(
            LeaderMode::Following {
                leader: 2,
                path: vec![2, 1],
                via: 2,
            }
            .vote(1),
            Some(2)
        );
    }

    #[tokio::test]
    async fn term_change_clears_stale_following_before_evaluating_new_term() {
        let state = node_state(1, true, HashMap::new(), 2);
        assert!(state.leader_status.follow_remote(2, vec![2, 1], 2).await);
        let mut task = CurrentLeaderTask::new(state.clone(), timing());
        task.last_considered_term = 1;

        task.tick().await;

        assert!(!matches!(state.leader_status.snapshot().await.mode, LeaderMode::Following { leader: 2, .. }));
    }

    #[test]
    fn node_state_term_is_single_term_source_for_tests() {
        let state = node_state(1, true, HashMap::new(), 7);

        assert_eq!(state.election_term.load(Ordering::Acquire), 7);
    }
}
