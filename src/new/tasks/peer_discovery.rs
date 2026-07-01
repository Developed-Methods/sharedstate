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

use futures_util::{stream, StreamExt};
use message_encoding::MessageEncoding;
use tokio::sync::Mutex;

use crate::{
    new::{
        election::{decide_election, ElectionDecision, ElectionInput, PeerReachability, TimedPeerObservation},
        node_state::{NodeState, PeerState},
        subscribable_state::StateHandle,
        tasks::peer_connections::PeerConnections,
    },
    protocol::messages::{SharePeerDetails, SyncRequest, SyncResponse},
    state::{determinstic_state::DeterministicState, recoverable_state::RecoverableStateAction},
    transport::traits::{SyncIO, SyncIOAddress},
    utils::now_ms,
};

static GENERATION_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct PeerDiscoveryElectionTask<I: SyncIO, D: DeterministicState> {
    state: Arc<NodeState<I::Address, D>>,
    peer_connections: Arc<PeerConnections<I, D>>,
    timing: PeerDiscoveryElectionTiming,
    state_handle: Mutex<StateHandle<D>>,
}

#[derive(Clone, Debug)]
pub struct PeerDiscoveryElectionTiming {
    pub observation_interval: Duration,
    pub observation_stale_after: Duration,
    pub max_concurrent_observations: usize,
}

impl Default for PeerDiscoveryElectionTiming {
    fn default() -> Self {
        Self {
            observation_interval: Duration::from_secs(3),
            observation_stale_after: Duration::from_secs(15),
            max_concurrent_observations: 8,
        }
    }
}

impl<I, D> PeerDiscoveryElectionTask<I, D>
where
    I: SyncIO,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    pub fn new(
        state: Arc<NodeState<I::Address, D>>,
        peer_connections: Arc<PeerConnections<I, D>>,
        timing: PeerDiscoveryElectionTiming,
    ) -> Self {
        let state_handle = Mutex::new(state.state.create_handle());
        Self {
            state,
            peer_connections,
            timing,
            state_handle,
        }
    }

    pub async fn run(self) {
        tracing::debug!(
            local = ?self.state.my_address,
            interval_ms = self.timing.observation_interval.as_millis(),
            stale_after_ms = self.timing.observation_stale_after.as_millis(),
            max_concurrent = self.timing.max_concurrent_observations,
            "starting peer discovery and election task",
        );

        loop {
            self.tick().await;
            tokio::time::sleep(self.timing.observation_interval).await;
        }
    }

    pub async fn tick(&self) {
        tracing::debug!(local = ?self.state.my_address, "starting peer discovery election tick");
        self.observe_peers().await;
        self.apply_election().await;
        tracing::debug!(local = ?self.state.my_address, "finished peer discovery election tick");
    }

    pub async fn observe_peers(&self) {
        let targets = self.observation_targets().await;
        if targets.is_empty() {
            tracing::debug!(local = ?self.state.my_address, "no peer discovery observation targets");
            return;
        }

        let share_peers = self.local_share_peers().await;
        let max_concurrent = self.timing.max_concurrent_observations.max(1);
        tracing::debug!(
            local = ?self.state.my_address,
            ?targets,
            share_peer_count = share_peers.len(),
            max_concurrent,
            "observing peers",
        );

        stream::iter(targets.into_iter().map(|peer| {
            let state = self.state.clone();
            let peer_connections = self.peer_connections.clone();
            let share_peers = share_peers.clone();

            async move {
                observe_peer(state, peer_connections, peer, share_peers).await;
            }
        }))
        .buffer_unordered(max_concurrent)
        .collect::<Vec<_>>()
        .await;
    }

    pub async fn apply_election(&self) {
        let now = now_ms();
        let stale_after_ms = self.timing.observation_stale_after.as_millis() as u64;
        let recover_details = self.state_handle.lock().await.recover_details();

        let (known_can_lead, reachable_can_lead, peer_observations, peer_reachability) = {
            let peers = self.state.peers.lock().await;
            let mut known_can_lead = peers
                .values()
                .filter_map(|peer| (peer.can_lead == Some(true)).then_some(peer.addr))
                .collect::<BTreeSet<_>>();
            let mut reachable_can_lead = peers
                .values()
                .filter_map(|peer| (peer.is_connected && peer.can_lead == Some(true)).then_some(peer.addr))
                .collect::<Vec<_>>();

            if self.state.can_lead {
                known_can_lead.insert(self.state.my_address);
                reachable_can_lead.push(self.state.my_address);
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

            (known_can_lead, reachable_can_lead, peer_observations, peer_reachability)
        };

        let local_observation = self
            .state
            .leader_status
            .local_observation(self.state.can_lead, reachable_can_lead, recover_details)
            .await;
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

    async fn observation_targets(&self) -> Vec<I::Address> {
        let leader_path = self.state.leader_status.path_to_leader().await;
        let peers = self.state.peers.lock().await;

        let mut targets = if self.state.can_lead || leader_path.is_none() {
            peers.keys().copied().collect::<Vec<_>>()
        } else {
            let mut targets = leader_path.clone().unwrap_or_default();
            targets.extend(
                peers
                    .values()
                    .filter_map(|peer| (peer.can_lead == Some(true)).then_some(peer.addr)),
            );
            targets
        };

        targets.retain(|addr| *addr != self.state.my_address);
        targets.sort();
        targets.dedup();
        tracing::debug!(
            local = ?self.state.my_address,
            can_lead = self.state.can_lead,
            leader_path = ?leader_path,
            ?targets,
            "selected peer discovery observation targets",
        );
        targets
    }

    async fn local_share_peers(&self) -> Vec<SharePeerDetails<I::Address>> {
        let peers = self.state.peers.lock().await;
        let mut share = Vec::with_capacity(peers.len() + 1);
        share.push(SharePeerDetails {
            address: self.state.my_address,
            can_be_leader: Some(self.state.can_lead),
            last_global_activity: NonZeroU64::new(now_ms()),
        });
        share.extend(peers.values().map(|peer| SharePeerDetails {
            address: peer.addr,
            can_be_leader: peer.can_lead,
            last_global_activity: peer.last_global_connectivity,
        }));
        tracing::debug!(
            local = ?self.state.my_address,
            share_peer_count = share.len(),
            "built local peer discovery share list",
        );
        share
    }
}

async fn observe_peer<I, D>(
    state: Arc<NodeState<I::Address, D>>,
    peer_connections: Arc<PeerConnections<I, D>>,
    peer: I::Address,
    share_peers: Vec<SharePeerDetails<I::Address>>,
) where
    I: SyncIO,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    tracing::debug!(local = ?state.my_address, ?peer, "observing peer");
    let started = now_ms();
    match peer_connections.send_rpc(peer, SyncRequest::Ping(started)).await {
        Ok(SyncResponse::Pong(id)) if id == started => {
            let latency = NonZeroU64::new(now_ms().saturating_sub(started).max(1));
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                latency_ms = latency.map(NonZeroU64::get),
                "peer ping succeeded",
            );
            mark_peer_observed(&state, peer, latency).await;
        }
        Ok(response) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                response = response_name(&response),
                "peer ping returned unexpected response",
            );
            peer_connections.kill_connection(peer).await;
            clear_failed_observation(&state, peer).await;
            return;
        }
        Err(error) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                ?error,
                "peer ping failed",
            );
            clear_failed_observation(&state, peer).await;
            return;
        }
    }

    match peer_connections
        .send_rpc(peer, SyncRequest::SharePeers(share_peers))
        .await
    {
        Ok(SyncResponse::Peers(peers)) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                peer_count = peers.len(),
                "peer shared peer details",
            );
            merge_peer_details(&state, peers).await;
        }
        Ok(response) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                response = response_name(&response),
                "share peers returned unexpected response",
            );
            peer_connections.kill_connection(peer).await;
            clear_failed_observation(&state, peer).await;
            return;
        }
        Err(error) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                ?error,
                "share peers request failed",
            );
            clear_failed_observation(&state, peer).await;
            return;
        }
    }

    match peer_connections.send_rpc(peer, SyncRequest::ShareLeaderInfo).await {
        Ok(SyncResponse::LeaderInfo(info)) if info.observer == peer => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                observer = ?info.observer,
                leader = ?info.leader,
                leader_path = ?info.leader_path,
                term = info.term,
                can_lead = info.can_lead,
                reachable_can_lead = ?info.reachable_can_lead,
                "peer shared leader info",
            );
            let mut peers = state.peers.lock().await;
            let peer_state = peers.entry(peer).or_insert_with(|| empty_peer_state(peer));
            peer_state.can_lead = Some(info.can_lead);
            peer_state.last_global_connectivity = NonZeroU64::new(now_ms());
            peer_state.leader_observation = Some(info);
        }
        Ok(response) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                response = response_name(&response),
                "share leader info returned unexpected response",
            );
            peer_connections.kill_connection(peer).await;
            clear_failed_observation(&state, peer).await;
        }
        Err(error) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                ?error,
                "share leader info request failed",
            );
            clear_failed_observation(&state, peer).await;
        }
    }
}

async fn mark_peer_observed<A, D>(state: &NodeState<A, D>, peer: A, latency: Option<NonZeroU64>)
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    let mut peers = state.peers.lock().await;
    let peer_state = peers.entry(peer).or_insert_with(|| empty_peer_state(peer));
    peer_state.latency = latency;
    peer_state.last_global_connectivity = NonZeroU64::new(now_ms());
    tracing::debug!(
        local = ?state.my_address,
        ?peer,
        latency_ms = latency.map(NonZeroU64::get),
        "marked peer connected",
    );
}

async fn clear_failed_observation<A, D>(state: &NodeState<A, D>, peer: A)
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    {
        let mut peers = state.peers.lock().await;
        let peer_state = peers.entry(peer).or_insert_with(|| empty_peer_state(peer));
        peer_state.leader_observation = None;
    }
    let cleared_via = state.leader_status.clear_if_via(peer).await;
    let cleared_leader = state.leader_status.clear_if_leader(peer).await;
    tracing::debug!(
        local = ?state.my_address,
        ?peer,
        cleared_via,
        cleared_leader,
        "cleared failed peer observation",
    );
}

async fn merge_peer_details<A, D>(state: &NodeState<A, D>, shared_peers: Vec<SharePeerDetails<A>>)
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    let mut peers = state.peers.lock().await;
    let shared_count = shared_peers.len();
    let mut inserted = 0usize;
    let mut updated = 0usize;
    for shared in shared_peers {
        if shared.address == state.my_address {
            continue;
        }

        let peer_state = peers.entry(shared.address).or_insert_with(|| {
            inserted += 1;
            empty_peer_state(shared.address)
        });
        updated += 1;
        if let Some(can_lead) = shared.can_be_leader {
            peer_state.can_lead = Some(can_lead);
        }
        peer_state.last_global_connectivity = match (peer_state.last_global_connectivity, shared.last_global_activity) {
            (None, Some(activity)) | (Some(activity), None) => Some(activity),
            (Some(a), Some(b)) => Some(a.max(b)),
            (None, None) => None,
        };
    }
    tracing::debug!(
        local = ?state.my_address,
        shared_count,
        inserted,
        updated,
        "merged shared peer details",
    );
}

fn response_name<A: SyncIOAddress, D: DeterministicState>(response: &SyncResponse<A, D>) -> &'static str {
    match response {
        SyncResponse::Pong(_) => "Pong",
        SyncResponse::Ok => "Ok",
        SyncResponse::FailedToQueueAction { .. } => "FailedToQueueAction",
        SyncResponse::Peers(_) => "Peers",
        SyncResponse::LeaderInfo(_) => "LeaderInfo",
        SyncResponse::LeaderPath(_) => "LeaderPath",
        SyncResponse::NoPathToLeader => "NoPathToLeader",
        SyncResponse::Accepted(_) => "Accepted",
        SyncResponse::RecoveryFailed => "RecoveryFailed",
        SyncResponse::FreshState(_) => "FreshState",
        SyncResponse::AuthorityAction(_, _) => "AuthorityAction",
        SyncResponse::ActionStreamClosed => "ActionStreamClosed",
        SyncResponse::UnexpectedRequest => "UnexpectedRequest",
    }
}

fn empty_peer_state<A: SyncIOAddress>(addr: A) -> PeerState<A> {
    PeerState {
        addr,
        latency: None,
        can_lead: None,
        is_connected: false,
        last_global_connectivity: None,
        leader_observation: None,
    }
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
    use std::{collections::HashMap, io::Result, sync::Arc, time::Duration};

    use message_encoding::MessageEncoding;
    use sequenced_broadcast::SequencedBroadcastSettings;
    use tokio::{
        io::{DuplexStream, ReadHalf, WriteHalf},
        sync::Mutex,
    };

    use super::*;
    use crate::{
        new::{
            subscribable_state::SubscribableState,
            tasks::{
                current_leader::{CurrentLeaderStatus, LeaderMode},
                peer_connections::PeerConnections,
            },
        },
        protocol::messages::LeaderWithElectionInfo,
        state::recoverable_state::{RecoverableState, RecoverableStateDetails},
        transport::{channels::NetIoSettings, traits::SyncConnection},
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

    impl MessageEncoding for TestState {
        fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
            self.0.write_to(out)
        }

        fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
            Ok(Self(MessageEncoding::read_from(read)?))
        }
    }

    impl MessageEncoding for TestAction {
        fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
            self.0.write_to(out)
        }

        fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
            Ok(Self(MessageEncoding::read_from(read)?))
        }
    }

    struct TestIo;

    impl SyncIO for TestIo {
        type Address = u64;
        type Read = ReadHalf<DuplexStream>;
        type Write = WriteHalf<DuplexStream>;

        async fn connect(&self, _remote: &Self::Address) -> Result<SyncConnection<Self>> {
            unreachable!("peer discovery unit tests do not open network connections")
        }
    }

    fn settings() -> NetIoSettings {
        NetIoSettings {
            process_timeout: Duration::from_millis(100),
            message_timeout: Duration::from_millis(100),
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

    fn task(state: Arc<NodeState<u64, TestState>>) -> PeerDiscoveryElectionTask<TestIo, TestState> {
        let connections = Arc::new(PeerConnections::new(Arc::new(TestIo), settings(), state.clone()));
        PeerDiscoveryElectionTask::new(state, connections, PeerDiscoveryElectionTiming::default())
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

        clear_failed_observation(&state, 2).await;

        assert_eq!(state.leader_status.snapshot().await.mode, LeaderMode::NoLeader { term: 3 });
        let peers = state.peers.lock().await;
        assert!(peers.get(&2).is_some_and(|peer| peer.is_connected));
    }

    #[tokio::test]
    async fn discovers_peers_from_share_peers() {
        let state = node_state(1, true, HashMap::new());

        merge_peer_details(
            &state,
            vec![SharePeerDetails {
                address: 2,
                can_be_leader: Some(true),
                last_global_activity: NonZeroU64::new(10),
            }],
        )
        .await;

        let peers = state.peers.lock().await;
        let peer = peers.get(&2).unwrap();
        assert_eq!(peer.can_lead, Some(true));
        assert_eq!(peer.last_global_connectivity, NonZeroU64::new(10));
    }
}
