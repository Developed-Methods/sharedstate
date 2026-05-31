use std::{
    collections::hash_map::DefaultHasher,
    collections::{hash_map, BTreeSet, HashMap},
    future::Future,
    hash::Hasher,
    num::NonZeroU64,
    panic::Location,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use futures_util::StreamExt;
use message_encoding::MessageEncoding;
use tokio::{
    sync::{mpsc, watch, Mutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    net::{
        message_channel::NetIoSettings,
        sync_io::{SyncConnection, SyncIO, SyncIOAddress, SyncIOListener},
    },
    shared::{
        authorative_state::AuthorativeState,
        messages::{ElectionObservation, LeaderInfoMessage, LeaderStatus, SharePeerDetails, SyncRequest, SyncResponse},
    },
    state::{
        determinstic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction},
        shared_state::{SharedStateHandle, SharedStateReader},
    },
    utils::now_ms,
};

static PROMOTION_COUNTER: AtomicU64 = AtomicU64::new(1);
const PROTOCOL_VERSION: u64 = 1;

mod client_policy;
mod election;
mod follow_policy;
mod paths;
mod timing;

use client_policy::{observation_targets, ObservationTargetInput, PeerKind};
use election::{
    decide_election, ElectionDecision, ElectionInput, ElectionState, PeerReachability, TimedPeerObservation,
};
use follow_policy::{sort_follow_candidates, FollowCandidate};
use paths::{append_path, valid_local_leader_path, valid_remote_leader_path};
pub use timing::NodeTiming;

pub struct NodeState<A: SyncIOAddress, D: DeterministicState> {
    inner: Arc<Inner<A, D>>,
    io_settings: NetIoSettings,
}

#[derive(Debug)]
pub enum SendActionError {
    Closed,
}

#[derive(Clone)]
pub struct NodeActionSender<Action> {
    tx: mpsc::Sender<Action>,
}

impl<Action> NodeActionSender<Action> {
    pub async fn send(&self, action: Action) -> Result<(), SendActionError> {
        self.tx.send(action).await.map_err(|_| SendActionError::Closed)
    }
}

#[derive(Debug)]
pub struct NodeDebugInfo<A: SyncIOAddress> {
    pub address: A,
    pub can_lead: bool,
    pub leader: Option<A>,
    pub leader_path: Option<Vec<A>>,
    pub term: u64,
    pub follow_remote: Option<A>,
    pub follow_leader_path: Option<Vec<A>>,
    pub known_can_lead: Vec<A>,
    pub last_promoted_leader: Option<A>,
    pub observations: Vec<ElectionObservation<A>>,
    pub peers: Vec<PeerDebugInfo<A>>,
}

#[derive(Debug)]
pub struct PeerDebugInfo<A: SyncIOAddress> {
    pub address: A,
    pub known: bool,
    pub can_lead: Option<bool>,
    pub connected: Option<bool>,
    pub latency_ms: Option<u64>,
    pub repeat_connect_fails: Option<u64>,
    pub last_activity_ms_ago: Option<u64>,
    pub last_global_activity_ms_ago: Option<u64>,
    pub last_connect_attempt_ms_ago: Option<u64>,
    pub last_connect_fail_ms_ago: Option<u64>,
    pub observed_leader: Option<A>,
    pub observed_term: Option<u64>,
    pub observed_leader_path: Option<Vec<A>>,
    pub observed_reachable_can_lead: Option<Vec<A>>,
}

struct Inner<A: SyncIOAddress, D: DeterministicState> {
    address: A,
    can_lead: bool,
    timing: NodeTiming,
    control: Mutex<ControlState<A, D>>,
    state: Mutex<AuthorativeState<D>>,
    state_reader: SharedStateReader<RecoverableState<D>>,
    local_actions_tx: mpsc::Sender<D::Action>,
    local_actions_rx: Mutex<Option<mpsc::Receiver<D::Action>>>,
    leader_updates: watch::Sender<LeaderInfoMessage<A>>,
}

struct ControlState<A: SyncIOAddress, D: DeterministicState> {
    leader: LeaderInfo<A>,
    peers: HashMap<A, Option<PeerDetails<A>>>,
    follow: Option<FollowConnection<A, D>>,
    election: ElectionState<A>,
}

#[derive(Clone)]
struct LeaderInfo<A: SyncIOAddress> {
    leader: Option<A>,
    path: Option<Vec<A>>,
    term: u64,
}

#[derive(Clone)]
struct PeerDetails<A: SyncIOAddress> {
    last_activity: Option<NonZeroU64>,
    last_global_activity: Option<NonZeroU64>,
    last_connect_attempt: Option<NonZeroU64>,
    last_connect_fail: Option<NonZeroU64>,
    repeat_connect_fails: u64,
    latency_ms: Option<u64>,
    can_lead: bool,
    connected: bool,
    last_observation: Option<ElectionObservation<A>>,
}

struct FollowConnection<A: SyncIOAddress, D: DeterministicState> {
    remote: A,
    leader_path: Vec<A>,
    to_peer: mpsc::Sender<SyncRequest<A, D>>,
    cancel: CancellationToken,
}

impl<A: SyncIOAddress, D: DeterministicState> NodeState<A, D>
where
    D::Action: Clone,
{
    pub async fn new(address: A, init_state: RecoverableState<D>, can_lead: bool, io_settings: NetIoSettings) -> Self {
        Self::new_with_timing(address, init_state, can_lead, io_settings, NodeTiming::default()).await
    }

    pub async fn new_with_timing(
        address: A,
        init_state: RecoverableState<D>,
        can_lead: bool,
        io_settings: NetIoSettings,
        timing: NodeTiming,
    ) -> Self {
        let (local_actions_tx, local_actions_rx) = mpsc::channel(1024);
        let (leader_updates, _) = watch::channel(LeaderInfoMessage {
            leader: None,
            path: None,
            term: 0,
        });
        let state = AuthorativeState::new(init_state).await;
        let state_reader = state.state_reader();

        NodeState {
            inner: Arc::new(Inner {
                address,
                can_lead,
                timing,
                control: Mutex::new(ControlState {
                    leader: LeaderInfo {
                        leader: None,
                        path: None,
                        term: 0,
                    },
                    peers: {
                        let mut map = HashMap::new();
                        map.insert(
                            address,
                            Some(PeerDetails {
                                last_activity: None,
                                last_connect_attempt: None,
                                last_connect_fail: None,
                                last_global_activity: None,
                                repeat_connect_fails: 0,
                                latency_ms: Some(0),
                                can_lead,
                                connected: true,
                                last_observation: None,
                            }),
                        );

                        map
                    },
                    follow: None,
                    election: ElectionState {
                        term: 0,
                        known_can_lead: {
                            let mut set = BTreeSet::new();
                            if can_lead {
                                set.insert(address);
                            }
                            set
                        },
                        observations: HashMap::new(),
                        last_promoted_leader: None,
                    },
                }),
                state: Mutex::new(state),
                state_reader,
                local_actions_tx,
                local_actions_rx: Mutex::new(Some(local_actions_rx)),
                leader_updates,
            }),
            io_settings,
        }
    }

    pub async fn discover_peers(&self, peers: impl Iterator<Item = A>) {
        self.inner.discover_peers(peers).await;
    }

    pub fn state_reader(&self) -> SharedStateReader<RecoverableState<D>> {
        self.inner.state_reader.clone()
    }

    pub fn create_state_handle(&self) -> SharedStateHandle<RecoverableState<D>> {
        self.inner.state_reader.create_handle()
    }

    pub fn action_sender(&self) -> NodeActionSender<D::Action> {
        NodeActionSender {
            tx: self.inner.local_actions_tx.clone(),
        }
    }

    pub async fn debug_info(&self) -> NodeDebugInfo<A> {
        self.inner.debug_info().await
    }

    pub async fn start_client<I>(&self, io: Arc<I>) -> JoinHandle<()>
    where
        I: SyncIO<Address = A>,
        D: MessageEncoding,
        D::Action: MessageEncoding + Clone,
        D::AuthorityAction: MessageEncoding,
    {
        self.inner.start_local_action_pump().await;

        tokio::spawn(
            ClientWorker::<I, D> {
                inner: self.inner.clone(),
                io,
                io_settings: self.io_settings.clone(),
            }
            .run(),
        )
    }

    pub async fn start_listener<I>(&self, io: Arc<I>) -> JoinHandle<()>
    where
        I: SyncIOListener<Address = A>,
        D: MessageEncoding,
        D::Action: MessageEncoding + Clone,
        D::AuthorityAction: MessageEncoding,
    {
        let inner = self.inner.clone();
        let io_settings = self.io_settings.clone();

        tokio::spawn(async move {
            loop {
                match io.next_client().await {
                    Ok(conn) => {
                        let (addr, write, read) = conn.server_channels(io_settings.clone());
                        tokio::spawn(
                            PeerWorker::<A, D> {
                                addr,
                                inner: inner.clone(),
                                write,
                                read,
                            }
                            .run(),
                        );
                    }
                    Err(error) => {
                        tracing::warn!(?error, "listener stopped accepting clients");
                        break;
                    }
                }
            }
        })
    }

    pub async fn handle_client<I: SyncIO<Address = A>>(&self, conn: SyncConnection<I>) -> JoinHandle<()>
    where
        D: MessageEncoding,
        D::Action: MessageEncoding + Clone,
        D::AuthorityAction: MessageEncoding,
    {
        let (addr, write, read) = conn.server_channels(self.io_settings.clone());

        tokio::spawn(
            PeerWorker::<A, D> {
                addr,
                inner: self.inner.clone(),
                write,
                read,
            }
            .run(),
        )
    }
}

impl<A: SyncIOAddress, D: DeterministicState> Inner<A, D> {
    async fn start_local_action_pump(self: &Arc<Self>)
    where
        D::Action: Send,
    {
        let Some(mut rx) = self.local_actions_rx.lock().await.take() else {
            return;
        };

        let inner = self.clone();

        tokio::spawn(async move {
            let mut state_handle = inner.state.lock().await.create_state_handle();

            while let Some(mut action) = rx.recv().await {
                loop {
                    let is_leader = inner.control.lock().await.leader.leader == Some(inner.address);
                    if is_leader {
                        let authority = {
                            let state = state_handle.read();
                            let authority = state.authority(RecoverableStateAction::StateAction { action });
                            state_handle.quiescent();
                            authority
                        };

                        inner.state.lock().await.apply_authority(authority).await;
                        break;
                    }

                    let follow_tx = {
                        let control = inner.control.lock().await;
                        control.follow.as_ref().map(|follow| follow.to_peer.clone())
                    };

                    if let Some(follow_tx) = follow_tx {
                        let to_send = SyncRequest::Action {
                            source: inner.address,
                            action,
                        };

                        match follow_tx.send(to_send).await {
                            Ok(()) => break,
                            Err(error) => {
                                action = match error.0 {
                                    SyncRequest::Action { action, .. } => action,
                                    _ => unreachable!("sent action request"),
                                };
                            }
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        });
    }

    pub async fn discover_peers(&self, peers: impl Iterator<Item = impl Into<SharePeerDetails<A>>>) {
        let mut control = self.control.lock().await;

        for peer in peers {
            let details = peer.into();

            if details.address == self.address {
                continue;
            }

            if let Some(can_lead) = details.can_be_leader {
                if can_lead {
                    control.election.known_can_lead.insert(details.address);
                } else {
                    control.election.known_can_lead.remove(&details.address);
                }
            }

            match control.peers.entry(details.address) {
                hash_map::Entry::Vacant(v) => {
                    v.insert(details.can_be_leader.map(|can_lead| PeerDetails {
                        last_activity: None,
                        last_connect_attempt: None,
                        last_connect_fail: None,
                        repeat_connect_fails: 0,
                        latency_ms: None,
                        can_lead,
                        last_global_activity: details.last_global_activity,
                        connected: false,
                        last_observation: None,
                    }));
                }
                hash_map::Entry::Occupied(o) => {
                    let value = o.into_mut();

                    if let Some(can_lead) = details.can_be_leader {
                        if let Some(current) = value {
                            current.can_lead = can_lead;
                            if details.last_global_activity > current.last_global_activity {
                                current.last_global_activity = details.last_global_activity;
                            }
                            continue;
                        }

                        value.replace(PeerDetails {
                            last_activity: None,
                            last_connect_attempt: None,
                            last_connect_fail: None,
                            last_global_activity: details.last_global_activity,
                            repeat_connect_fails: 0,
                            latency_ms: None,
                            can_lead,
                            connected: false,
                            last_observation: None,
                        });
                    }
                }
            }
        }
    }

    async fn peer_snapshot(&self) -> Vec<SharePeerDetails<A>> {
        let control = self.control.lock().await;

        control
            .peers
            .iter()
            .map(|(address, details)| SharePeerDetails {
                address: *address,
                can_be_leader: details.as_ref().map(|v| v.can_lead),
                last_global_activity: details.as_ref().and_then(|v| v.last_global_activity),
            })
            .collect::<Vec<_>>()
    }

    async fn leader_info_message(&self) -> LeaderInfoMessage<A> {
        let control = self.control.lock().await;
        let leader = control.leader.clone();
        let follow_path = control.follow.as_ref().map(|follow| follow.leader_path.clone());
        let (leader_addr, path) = match leader.leader {
            Some(addr) if addr == self.address => (Some(addr), leader.path),
            Some(addr) => match follow_path {
                Some(path) => (Some(addr), Some(path)),
                None => (None, None),
            },
            None => (None, None),
        };

        LeaderInfoMessage {
            leader: leader_addr,
            path,
            term: leader.term,
        }
    }

    async fn publish_leader_info(&self) {
        let info = self.leader_info_message().await;
        let _ = self.leader_updates.send(info);
    }

    async fn debug_info(&self) -> NodeDebugInfo<A> {
        let now = now_ms();
        let control = self.control.lock().await;
        let leader = control.leader.clone();
        let follow = control
            .follow
            .as_ref()
            .map(|follow| (follow.remote, follow.leader_path.clone()));

        let mut peer_debug = control
            .peers
            .iter()
            .map(|(address, details)| {
                let observation = details.as_ref().and_then(|details| details.last_observation.clone());
                PeerDebugInfo {
                    address: *address,
                    known: details.is_some(),
                    can_lead: details.as_ref().map(|details| details.can_lead),
                    connected: details.as_ref().map(|details| details.connected),
                    latency_ms: details.as_ref().and_then(|details| details.latency_ms),
                    repeat_connect_fails: details.as_ref().map(|details| details.repeat_connect_fails),
                    last_activity_ms_ago: details
                        .as_ref()
                        .and_then(|details| details.last_activity)
                        .map(|ts| now.saturating_sub(ts.get())),
                    last_global_activity_ms_ago: details
                        .as_ref()
                        .and_then(|details| details.last_global_activity)
                        .map(|ts| now.saturating_sub(ts.get())),
                    last_connect_attempt_ms_ago: details
                        .as_ref()
                        .and_then(|details| details.last_connect_attempt)
                        .map(|ts| now.saturating_sub(ts.get())),
                    last_connect_fail_ms_ago: details
                        .as_ref()
                        .and_then(|details| details.last_connect_fail)
                        .map(|ts| now.saturating_sub(ts.get())),
                    observed_leader: observation.as_ref().and_then(|observation| observation.leader),
                    observed_term: observation.as_ref().map(|observation| observation.term),
                    observed_leader_path: observation
                        .as_ref()
                        .and_then(|observation| observation.leader_path.clone()),
                    observed_reachable_can_lead: observation.map(|observation| observation.reachable_can_lead),
                }
            })
            .collect::<Vec<_>>();
        peer_debug.sort_by_key(|peer| peer.address);

        let mut observations = control.election.observations.values().cloned().collect::<Vec<_>>();
        observations.sort_by_key(|observation| observation.observer);

        NodeDebugInfo {
            address: self.address,
            can_lead: self.can_lead,
            leader: leader.leader,
            leader_path: leader.path,
            term: leader.term,
            follow_remote: follow.as_ref().map(|(remote, _)| *remote),
            follow_leader_path: follow.map(|(_, path)| path),
            known_can_lead: control.election.known_can_lead.iter().copied().collect(),
            last_promoted_leader: control.election.last_promoted_leader,
            observations,
            peers: peer_debug,
        }
    }

    async fn local_observation(&self) -> ElectionObservation<A> {
        let (term, leader_addr, leader_path, reachable_can_lead) = {
            let control = self.control.lock().await;
            let leader = control.leader.clone();
            let follow_path = control.follow.as_ref().map(|follow| follow.leader_path.clone());
            let (leader_addr, leader_path) = match leader.leader {
                Some(addr) if addr == self.address => (Some(addr), leader.path),
                Some(addr) => match follow_path {
                    Some(path) => (Some(addr), Some(path)),
                    None => (None, None),
                },
                None => (None, None),
            };
            let reachable_can_lead = control
                .peers
                .iter()
                .filter_map(|(addr, details)| {
                    let details = details.as_ref()?;
                    if details.can_lead && (details.connected || *addr == self.address) {
                        Some(*addr)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            (leader.term, leader_addr, leader_path, reachable_can_lead)
        };
        let state_accept_seq = self.state.lock().await.state_clone().await.state().accept_seq();

        ElectionObservation {
            observer: self.address,
            term,
            leader: leader_addr,
            leader_path,
            can_lead: self.can_lead,
            reachable_can_lead,
            state_accept_seq,
        }
    }

    async fn record_observation(&self, mut observation: ElectionObservation<A>) {
        let mut control = self.control.lock().await;
        if observation.can_lead {
            control.election.known_can_lead.insert(observation.observer);
        }

        if let Some(leader) = observation.leader {
            if leader != observation.observer {
                let has_valid_relay_path = observation
                    .leader_path
                    .as_deref()
                    .map(|path| valid_remote_leader_path(Some(leader), path, self.address))
                    .unwrap_or(false);
                let leader_is_failed = control
                    .peers
                    .get(&leader)
                    .and_then(|details| details.as_ref())
                    .map(|details| {
                        !details.connected && details.last_activity.is_none() && details.last_connect_fail.is_some()
                    })
                    .unwrap_or(false);

                if leader_is_failed && !has_valid_relay_path {
                    observation.leader = None;
                    observation.leader_path = None;
                }
            }
        }

        control.election.term = control.election.term.max(observation.term);
        control
            .election
            .observations
            .insert(observation.observer, observation.clone());

        let details = control
            .peers
            .entry(observation.observer)
            .or_insert_with(|| {
                Some(PeerDetails {
                    last_activity: None,
                    last_connect_attempt: None,
                    last_connect_fail: None,
                    last_global_activity: None,
                    repeat_connect_fails: 0,
                    latency_ms: None,
                    can_lead: observation.can_lead,
                    connected: false,
                    last_observation: None,
                })
            })
            .get_or_insert_with(|| PeerDetails {
                last_activity: None,
                last_connect_attempt: None,
                last_connect_fail: None,
                last_global_activity: None,
                repeat_connect_fails: 0,
                latency_ms: None,
                can_lead: observation.can_lead,
                connected: false,
                last_observation: None,
            });

        details.can_lead = observation.can_lead;
        details.last_activity = NonZeroU64::new(now_ms());
        details.last_observation = Some(observation);
    }

    async fn apply_election(&self) {
        let local_observation = self.local_observation().await;
        let now = now_ms();
        let (known_can_lead, observations, local_term, peer_details) = {
            let control = self.control.lock().await;
            (
                control.election.known_can_lead.clone(),
                control.election.observations.clone(),
                control.election.term,
                control.peers.clone(),
            )
        };
        let max_seen_term = std::iter::once(&local_observation)
            .chain(observations.values())
            .map(|observation| observation.term)
            .max()
            .unwrap_or(local_term);

        self.observe_term(max_seen_term).await;

        let peer_observations = observations
            .into_iter()
            .map(|(observer, observation)| TimedPeerObservation {
                observer,
                last_activity_ms: peer_details
                    .get(&observer)
                    .and_then(|details| details.as_ref())
                    .and_then(|details| details.last_activity)
                    .map(|ts| ts.get()),
                observation,
            })
            .collect::<Vec<_>>();
        let peer_reachability = peer_details
            .iter()
            .filter_map(|(addr, details)| {
                Some((
                    *addr,
                    PeerReachability {
                        last_activity_ms: details
                            .as_ref()
                            .and_then(|details| details.last_activity)
                            .map(|ts| ts.get()),
                    },
                ))
            })
            .collect::<HashMap<_, _>>();

        match decide_election(ElectionInput {
            local_address: self.address,
            can_lead: self.can_lead,
            known_can_lead,
            local_observation,
            peer_observations,
            peer_reachability,
            election_term: local_term,
            now_ms: now,
            stale_after_ms: self.timing.observation_stale_ms(),
        }) {
            ElectionDecision::PromoteSelf { observed_term } => {
                self.promote_if_needed(observed_term).await;
            }
            ElectionDecision::FollowRemote { leader, term, path } => {
                let changed = {
                    let mut control = self.control.lock().await;
                    if control.leader.term < term
                        || control.leader.leader != Some(leader)
                        || control.leader.path != Some(path.clone())
                    {
                        control.leader.term = term;
                        control.leader.leader = Some(leader);
                        control.leader.path = Some(path);
                        true
                    } else {
                        false
                    }
                };
                if changed {
                    self.publish_leader_info().await;
                }
            }
            ElectionDecision::ClearRemoteLeader { leader } => {
                self.clear_remote_leader_if(leader).await;
            }
            ElectionDecision::NoChange => {}
        }
    }

    async fn promote_if_needed(&self, observed_term: u64) {
        {
            let mut control = self.control.lock().await;
            if control.leader.leader == Some(self.address) && control.leader.path.as_deref() == Some(&[self.address]) {
                return;
            }

            let current_leader_term = control.leader.term;
            control.election.term = control
                .election
                .term
                .max(observed_term)
                .max(current_leader_term)
                .saturating_add(1);
            control.election.last_promoted_leader = Some(self.address);
            let new_term = control.election.term;
            control.leader.leader = Some(self.address);
            control.leader.path = Some(vec![self.address]);
            control.leader.term = new_term;
        }
        self.publish_leader_info().await;

        let new_id = generation_id(self.address);
        let mut state = self.state.lock().await;
        state
            .apply_authority(RecoverableStateAction::BumpGeneration { new_id })
            .await;
    }

    async fn clear_follow(&self) {
        if let Some(follow) = self.control.lock().await.follow.take() {
            follow.cancel.cancel();
        }
    }

    async fn observe_term(&self, term: u64) {
        let mut control = self.control.lock().await;
        control.election.term = control.election.term.max(term);
    }

    async fn clear_follow_to(&self, remote: A) -> bool {
        let follow = {
            let mut control = self.control.lock().await;
            if control.follow.as_ref().is_some_and(|follow| follow.remote == remote) {
                control.follow.take()
            } else {
                None
            }
        };

        if let Some(follow) = follow {
            follow.cancel.cancel();
            true
        } else {
            false
        }
    }

    async fn clear_remote_leader_if(&self, leader_addr: A) {
        let changed = {
            let mut control = self.control.lock().await;
            if control.leader.leader == Some(leader_addr) && control.leader.leader != Some(self.address) {
                control.leader.leader = None;
                control.leader.path = None;
                true
            } else {
                false
            }
        };
        if changed {
            self.publish_leader_info().await;
        }
    }

    async fn clear_remote_leader(&self) {
        let changed = {
            let mut control = self.control.lock().await;
            if control.leader.leader.is_some() && control.leader.leader != Some(self.address) {
                control.leader.leader = None;
                control.leader.path = None;
                true
            } else {
                false
            }
        };
        if changed {
            self.publish_leader_info().await;
        }
    }

    async fn set_remote_leader_path(&self, leader_addr: A, term: Option<u64>, path: Vec<A>, follow_remote: Option<A>) {
        let (changed_leader, changed_follow) = {
            let mut control = self.control.lock().await;
            let leader = &mut control.leader;
            let changed_leader = if let Some(term) = term {
                if leader.term > term {
                    false
                } else {
                    let changed =
                        leader.term != term || leader.leader != Some(leader_addr) || leader.path != Some(path.clone());
                    leader.term = term;
                    leader.leader = Some(leader_addr);
                    leader.path = Some(path.clone());
                    changed
                }
            } else {
                let changed = leader.leader != Some(leader_addr) || leader.path != Some(path.clone());
                leader.leader = Some(leader_addr);
                leader.path = Some(path.clone());
                changed
            };

            let changed_follow = if let Some(follow_remote) = follow_remote {
                if let Some(follow) = control.follow.as_mut().filter(|follow| follow.remote == follow_remote) {
                    if follow.leader_path != path {
                        follow.leader_path = path;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            };

            (changed_leader, changed_follow)
        };

        if changed_leader || changed_follow {
            self.publish_leader_info().await;
        }
        if let Some(term) = term {
            self.observe_term(term).await;
        }
    }
}

struct ClientWorker<I: SyncIO, D: DeterministicState> {
    inner: Arc<Inner<I::Address, D>>,
    io: Arc<I>,
    io_settings: NetIoSettings,
}

impl<I, D> ClientWorker<I, D>
where
    I: SyncIO,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding + Clone,
    D::AuthorityAction: MessageEncoding,
{
    async fn run(self) {
        loop {
            self.observe_election_peers().await;
            self.inner.apply_election().await;
            self.ensure_follow_connection().await;
            tokio::time::sleep(self.inner.timing.observation_interval).await;
        }
    }

    async fn observe_election_peers(&self) {
        let targets = self.observation_targets().await;

        let mut results = futures_util::stream::iter(targets.into_iter())
            .map(|target| self.observe_peer(target))
            .buffered(8);

        while results.next().await.is_some() {}
    }

    async fn observation_targets(&self) -> Vec<I::Address> {
        let control = self.inner.control.lock().await;
        let leader = control.leader.leader;
        let follow_remote = control.follow.as_ref().map(|follow| follow.remote);
        let has_usable_path = leader.is_some() && (leader == Some(self.inner.address) || follow_remote.is_some());

        observation_targets(ObservationTargetInput {
            local: self.inner.address,
            can_lead: self.inner.can_lead,
            leader,
            follow_remote,
            has_usable_path,
            peers: control
                .peers
                .iter()
                .map(|(addr, details)| {
                    (
                        *addr,
                        details
                            .as_ref()
                            .map(|details| PeerKind::Known {
                                can_lead: details.can_lead,
                            })
                            .unwrap_or(PeerKind::Unknown),
                    )
                })
                .collect(),
        })
    }

    async fn observe_peer(&self, target: I::Address) {
        self.mark_connect_attempt(target).await;

        let Ok(Ok(conn)) = tokio::time::timeout(self.inner.timing.rpc_timeout, self.io.connect(&target)).await else {
            self.mark_connect_fail(target).await;
            return;
        };

        let (_addr, write, mut read) = conn.client_channels::<D>(self.io_settings.clone());

        if !send_expect_ok(
            &write,
            &mut read,
            SyncRequest::ProtocolVersion(PROTOCOL_VERSION),
            self.inner.timing.rpc_timeout,
        )
        .await
        {
            self.mark_connect_fail(target).await;
            return;
        }

        if !send_expect_ok(&write, &mut read, SyncRequest::MyAddress(self.inner.address), self.inner.timing.rpc_timeout)
            .await
        {
            self.mark_connect_fail(target).await;
            return;
        }

        let started = now_ms();
        if write.send(SyncRequest::Ping(started)).await.is_err() {
            self.mark_connect_fail(target).await;
            return;
        }

        match recv_timeout(&mut read, self.inner.timing.rpc_timeout).await {
            Some(SyncResponse::Pong(id)) if id == started => {
                self.mark_connected(target, now_ms().saturating_sub(started)).await;
            }
            _ => {
                self.mark_connect_fail(target).await;
                return;
            }
        }

        let peers = self.inner.peer_snapshot().await;
        if write.send(SyncRequest::SharePeers(peers)).await.is_ok() {
            if let Some(SyncResponse::Peers(peers)) = recv_timeout(&mut read, self.inner.timing.rpc_timeout).await {
                self.inner.discover_peers(peers.into_iter()).await;
            }
        }

        if write.send(SyncRequest::ShareElection).await.is_ok() {
            if let Some(SyncResponse::Election(observation)) =
                recv_timeout(&mut read, self.inner.timing.rpc_timeout).await
            {
                self.inner.record_observation(observation).await;
            }
        }
    }

    async fn ensure_follow_connection(&self) {
        let (leader, has_follow) = {
            let control = self.inner.control.lock().await;
            (control.leader.leader, control.follow.is_some())
        };
        if leader == Some(self.inner.address) {
            self.inner.clear_follow().await;
            return;
        }

        if has_follow {
            return;
        }

        let Some(target) = self.select_follow_target(leader).await else {
            tokio::time::sleep(self.inner.timing.follow_retry_interval).await;
            return;
        };

        self.connect_follow(target, leader).await;
    }

    async fn select_follow_target(&self, leader: Option<I::Address>) -> Option<I::Address> {
        let control = self.inner.control.lock().await;
        let candidates = control
            .peers
            .iter()
            .filter_map(|(addr, details)| {
                if *addr == self.inner.address {
                    return None;
                }
                Some(match details {
                    Some(details) => FollowCandidate {
                        address: *addr,
                        connected: details.connected,
                        latency_ms: details.latency_ms,
                        repeat_connect_fails: details.repeat_connect_fails,
                        last_connect_fail_ms: details.last_connect_fail.map(|v| v.get()),
                        failed_without_activity: details.last_activity.is_none() && details.last_connect_fail.is_some(),
                        can_lead: details.can_lead,
                        observed_leader: details.last_observation.as_ref().and_then(|o| o.leader),
                    },
                    None => FollowCandidate {
                        address: *addr,
                        connected: false,
                        latency_ms: None,
                        repeat_connect_fails: 0,
                        last_connect_fail_ms: None,
                        failed_without_activity: false,
                        can_lead: false,
                        observed_leader: None,
                    },
                })
            })
            .collect::<Vec<_>>();

        sort_follow_candidates(leader, candidates)
            .first()
            .map(|candidate| candidate.address)
    }

    async fn connect_follow(&self, target: I::Address, selected_leader: Option<I::Address>) {
        self.mark_connect_attempt(target).await;

        let Ok(Ok(conn)) = tokio::time::timeout(self.inner.timing.rpc_timeout, self.io.connect(&target)).await else {
            self.mark_connect_fail(target).await;
            return;
        };

        let (remote, write, mut read) = conn.client_channels::<D>(self.io_settings.clone());
        if !send_expect_ok(
            &write,
            &mut read,
            SyncRequest::ProtocolVersion(PROTOCOL_VERSION),
            self.inner.timing.rpc_timeout,
        )
        .await
        {
            self.mark_connect_fail(target).await;
            return;
        }

        if !send_expect_ok(&write, &mut read, SyncRequest::MyAddress(self.inner.address), self.inner.timing.rpc_timeout)
            .await
        {
            self.mark_connect_fail(target).await;
            return;
        }

        let leader_info = if write.send(SyncRequest::WhoIsLeader).await.is_ok() {
            match recv_timeout(&mut read, self.inner.timing.rpc_timeout).await {
                Some(SyncResponse::LeaderInfo(info)) => info,
                _ => {
                    self.mark_connect_fail(target).await;
                    return;
                }
            }
        } else {
            self.mark_connect_fail(target).await;
            return;
        };

        let Some(leader) = leader_info.leader.or(selected_leader) else {
            self.mark_connect_fail(target).await;
            return;
        };

        let Some(path) = leader_info.path.clone() else {
            self.mark_connect_fail(target).await;
            return;
        };

        if !valid_remote_leader_path(Some(leader), &path, self.inner.address) {
            self.mark_connect_fail(target).await;
            return;
        }

        let local_path = append_path(path.clone(), self.inner.address);
        self.inner
            .set_remote_leader_path(leader, Some(leader_info.term), local_path.clone(), None)
            .await;

        let details = self.inner.state.lock().await.recoverable_state_details().await;
        if write.send(SyncRequest::SubscribeRecovery(details)).await.is_err() {
            self.mark_connect_fail(target).await;
            return;
        }

        let first_state_msg = match recv_timeout(&mut read, self.inner.timing.rpc_timeout).await {
            Some(SyncResponse::Accepted(_seq)) => None,
            Some(SyncResponse::RecoveryFailed(_)) => {
                if write.send(SyncRequest::SubscribeFresh).await.is_err() {
                    self.mark_connect_fail(target).await;
                    return;
                }
                match recv_timeout(&mut read, self.inner.timing.rpc_timeout).await {
                    Some(SyncResponse::FreshState(state)) => Some(state),
                    _ => {
                        self.mark_connect_fail(target).await;
                        return;
                    }
                }
            }
            Some(SyncResponse::FreshState(state)) => Some(state),
            _ => {
                self.mark_connect_fail(target).await;
                return;
            }
        };

        if let Some(state) = first_state_msg {
            self.inner.state.lock().await.reset(state).await;
        }

        let cancel = CancellationToken::new();
        let follow = FollowConnection {
            remote,
            leader_path: local_path,
            to_peer: write.clone(),
            cancel: cancel.clone(),
        };

        if let Some(existing) = self.inner.control.lock().await.follow.replace(follow) {
            existing.cancel.cancel();
        }

        self.mark_connected(target, 0).await;
        self.inner.publish_leader_info().await;
        self.spawn_follow_reader(target, read, cancel);
    }

    fn spawn_follow_reader(
        &self,
        target: I::Address,
        mut read: mpsc::Receiver<SyncResponse<I::Address, D>>,
        cancel: CancellationToken,
    ) {
        let inner = self.inner.clone();

        tokio::spawn(async move {
            loop {
                let msg = tokio::select! {
                    _ = cancel.cancelled() => break,
                    msg = read.recv() => msg,
                };

                match msg {
                    Some(SyncResponse::AuthorityAction(_, action)) => {
                        inner.state.lock().await.apply_authority(action).await;
                    }
                    Some(SyncResponse::LeaderInfo(info)) => match (info.leader, info.path) {
                        (Some(leader_addr), Some(path))
                            if valid_remote_leader_path(Some(leader_addr), &path, inner.address) =>
                        {
                            inner
                                .set_remote_leader_path(
                                    leader_addr,
                                    Some(info.term),
                                    append_path(path, inner.address),
                                    Some(target),
                                )
                                .await;
                        }
                        _ => {
                            if inner.clear_follow_to(target).await {
                                inner.clear_remote_leader().await;
                            }
                        }
                    },
                    Some(SyncResponse::LeaderPath(path)) => {
                        if let Some(leader_addr) = path.first().copied() {
                            if valid_remote_leader_path(Some(leader_addr), &path, inner.address) {
                                inner
                                    .set_remote_leader_path(
                                        leader_addr,
                                        None,
                                        append_path(path, inner.address),
                                        Some(target),
                                    )
                                    .await;
                            }
                        }
                    }
                    Some(SyncResponse::NoPathToLeader) => {
                        if inner.clear_follow_to(target).await {
                            inner.clear_remote_leader().await;
                        }
                    }
                    Some(SyncResponse::Peers(peers)) => {
                        inner.discover_peers(peers.into_iter()).await;
                    }
                    Some(SyncResponse::ActionStreamClosed) | None => break,
                    _ => {}
                }
            }

            let closed_follow = { inner.control.lock().await.follow.take() };
            if let Some(follow) = closed_follow {
                if follow.remote == target {
                    follow.cancel.cancel();
                    inner.clear_remote_leader().await;
                } else {
                    let _ = inner.control.lock().await.follow.replace(follow);
                }
            }
        });
    }

    async fn mark_connect_attempt(&self, target: I::Address) {
        let mut control = self.inner.control.lock().await;
        if let Some(Some(details)) = control.peers.get_mut(&target) {
            details.last_connect_attempt = NonZeroU64::new(now_ms());
        }
    }

    async fn mark_connect_fail(&self, target: I::Address) {
        {
            let mut control = self.inner.control.lock().await;
            if let Some(Some(details)) = control.peers.get_mut(&target) {
                details.last_activity = None;
                details.last_connect_fail = NonZeroU64::new(now_ms());
                details.repeat_connect_fails = details.repeat_connect_fails.saturating_add(1);
                details.connected = false;
                details.last_observation = None;
            }

            for details in control.peers.values_mut().filter_map(Option::as_mut) {
                let invalid_target_observation = details
                    .last_observation
                    .as_ref()
                    .filter(|observation| observation.leader == Some(target))
                    .map(|observation| {
                        !observation
                            .leader_path
                            .as_deref()
                            .map(|path| valid_remote_leader_path(Some(target), path, self.inner.address))
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);
                if invalid_target_observation {
                    details.last_observation = None;
                }
            }

            control.election.observations.retain(|observer, observation| {
                if *observer == target {
                    return false;
                }
                if observation.leader != Some(target) {
                    return true;
                }
                observation
                    .leader_path
                    .as_deref()
                    .map(|path| valid_remote_leader_path(Some(target), path, self.inner.address))
                    .unwrap_or(false)
            });
            let has_relay_follow = control
                .follow
                .as_ref()
                .is_some_and(|follow| follow.remote != target && follow.leader_path.first().copied() == Some(target));
            let should_clear = !has_relay_follow
                && control.leader.leader == Some(target)
                && control.leader.leader != Some(self.inner.address);
            if should_clear {
                control.leader.leader = None;
                control.leader.path = None;
            }
            drop(control);
            if should_clear {
                self.inner.publish_leader_info().await;
            }
        }
    }

    async fn mark_connected(&self, target: I::Address, latency_ms: u64) {
        let mut control = self.inner.control.lock().await;
        if let Some(Some(details)) = control.peers.get_mut(&target) {
            details.last_activity = NonZeroU64::new(now_ms());
            details.last_global_activity = NonZeroU64::new(now_ms());
            details.repeat_connect_fails = 0;
            details.latency_ms = Some(latency_ms);
            details.connected = true;
        }
    }
}

struct PeerWorker<A: SyncIOAddress, D: DeterministicState> {
    addr: A,
    inner: Arc<Inner<A, D>>,
    read: mpsc::Receiver<SyncRequest<A, D>>,
    write: mpsc::Sender<SyncResponse<A, D>>,
}

impl<A: SyncIOAddress, D: DeterministicState> PeerWorker<A, D>
where
    D::Action: Clone,
{
    async fn run(mut self) {
        while let Some(msg) = self.read.recv().await {
            match msg {
                SyncRequest::Ping(id) => {
                    self.record_activity_ts(true).await;

                    if !self.send(SyncResponse::Pong(id)).await {
                        break;
                    }
                }
                SyncRequest::ProtocolVersion(version) => {
                    if version != PROTOCOL_VERSION {
                        break;
                    }

                    if !self.send(SyncResponse::Ok).await {
                        break;
                    }
                }
                SyncRequest::SharePeers(peers) => {
                    self.inner.discover_peers(peers.into_iter()).await;
                    if !self.send(SyncResponse::Peers(self.inner.peer_snapshot().await)).await {
                        break;
                    }
                }
                SyncRequest::WhoIsLeader => {
                    if !self
                        .send(SyncResponse::LeaderInfo(self.inner.leader_info_message().await))
                        .await
                    {
                        break;
                    }
                }
                SyncRequest::ShareElection => {
                    let observation = self.inner.local_observation().await;
                    if !self.send(SyncResponse::Election(observation)).await {
                        break;
                    }
                }
                SyncRequest::SubscribeFresh => {
                    if !self.subscribe_fresh().await {
                        break;
                    }
                }
                SyncRequest::SubscribeRecovery(details) => {
                    if !self.subscribe_recovery(details).await {
                        break;
                    }
                }
                SyncRequest::MyAddress(addr) => {
                    self.addr = addr;
                    self.record_activity_ts(true).await;
                    if !self.send(SyncResponse::Ok).await {
                        break;
                    }
                }
                SyncRequest::ShareLeaderPath => {
                    let info = self.inner.leader_info_message().await;
                    let resp = match info.path {
                        None => SyncResponse::NoPathToLeader,
                        Some(path) if valid_local_leader_path(path.first().copied(), &path, self.inner.address) => {
                            SyncResponse::LeaderPath(path)
                        }
                        Some(_) => SyncResponse::NoPathToLeader,
                    };

                    if !self.send(resp).await {
                        break;
                    }
                }
                SyncRequest::Action { source, action } => {
                    self.handle_action(source, action).await;
                }
                SyncRequest::LeaderStatus { address, status } => {
                    self.handle_leader_status(address, status).await;

                    if !self.send(SyncResponse::Ok).await {
                        break;
                    }
                }
            };
        }

        self.record_activity_ts(false).await;
    }

    async fn subscribe_fresh(&self) -> bool {
        let (state, mut feed) = {
            let state = self.inner.state.lock().await;
            state.subscribe().await
        };
        if !self.send(SyncResponse::FreshState(state)).await {
            return false;
        }
        if !self
            .send(SyncResponse::LeaderInfo(self.inner.leader_info_message().await))
            .await
        {
            return false;
        }

        let write = self.write.clone();
        let mut leader_updates = self.inner.leader_updates.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    action = feed.recv() => {
                        match action {
                            Ok((seq, action)) => {
                                if write.send(SyncResponse::AuthorityAction(seq, action)).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    changed = leader_updates.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        let info = leader_updates.borrow().clone();
                        if write.send(SyncResponse::LeaderInfo(info)).await.is_err() {
                            break;
                        }
                    }
                }
            }

            let _ = write.send(SyncResponse::ActionStreamClosed).await;
        });

        true
    }

    async fn subscribe_recovery(&self, details: crate::state::recoverable_state::RecoverableStateDetails) -> bool {
        let leader_state = self.inner.state.lock().await.recoverable_state_details().await;

        if !leader_state.can_recover_follower(&details) {
            return self.send(SyncResponse::RecoveryFailed(leader_state)).await;
        }

        let feed = {
            let state = self.inner.state.lock().await;
            state.subscribe_at(details.next_seq()).await
        };

        let Ok(mut feed) = feed else {
            return self.send(SyncResponse::RecoveryFailed(leader_state)).await;
        };

        if !self.send(SyncResponse::Accepted(feed.next_seq())).await {
            return false;
        }
        if !self
            .send(SyncResponse::LeaderInfo(self.inner.leader_info_message().await))
            .await
        {
            return false;
        }

        let write = self.write.clone();
        let mut leader_updates = self.inner.leader_updates.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    action = feed.recv() => {
                        match action {
                            Ok((seq, action)) => {
                                if write.send(SyncResponse::AuthorityAction(seq, action)).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    changed = leader_updates.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        let info = leader_updates.borrow().clone();
                        if write.send(SyncResponse::LeaderInfo(info)).await.is_err() {
                            break;
                        }
                    }
                }
            }

            let _ = write.send(SyncResponse::ActionStreamClosed).await;
        });

        true
    }

    async fn handle_action(&self, source: A, action: D::Action) {
        let (is_leader, follow_tx) = {
            let control = self.inner.control.lock().await;
            (
                control.leader.leader == Some(self.inner.address),
                control.follow.as_ref().map(|follow| follow.to_peer.clone()),
            )
        };
        if is_leader {
            let authority = {
                let state = self.inner.state.lock().await;
                let state_clone = state.state_clone().await;
                state_clone.state().authority(action)
            };
            self.inner
                .state
                .lock()
                .await
                .apply_authority(RecoverableStateAction::StateAction { action: authority })
                .await;
            return;
        }

        if let Some(follow_tx) = follow_tx {
            if follow_tx.send(SyncRequest::Action { source, action }).await.is_err() {
                tracing::warn!("failed to forward action to upstream leader");
            }
            return;
        }

        tracing::warn!(?source, "dropping remote action because no leader path is available");
    }

    async fn handle_leader_status(&self, address: A, status: LeaderStatus<A>) {
        let mut changed = false;
        let mut observed_term = None;
        match status {
            LeaderStatus::Promoted { term, leader } => {
                observed_term = Some(term);
                let mut control = self.inner.control.lock().await;
                if control.leader.term < term || (control.leader.term == term && Some(leader) < control.leader.leader) {
                    control.leader.term = term;
                    control.leader.leader = Some(leader);
                    control.leader.path = if leader == self.inner.address {
                        Some(vec![self.inner.address])
                    } else if leader == address {
                        Some(vec![leader, self.inner.address])
                    } else {
                        None
                    };
                    changed = true;
                }
            }
            LeaderStatus::Offline { term, leader } => {
                observed_term = Some(term);
                let mut control = self.inner.control.lock().await;
                if control.leader.term <= term && control.leader.leader == Some(leader) {
                    control.leader.leader = None;
                    control.leader.path = None;
                    control.leader.term = term;
                    changed = true;
                }
            }
            LeaderStatus::Observation(observation) => {
                self.inner.record_observation(observation).await;
            }
        }
        if changed {
            self.inner.publish_leader_info().await;
        }
        if let Some(term) = observed_term {
            self.inner.observe_term(term).await;
        }
    }

    async fn record_activity_ts(&self, connected: bool) {
        let now = NonZeroU64::new(now_ms());

        let mut control = self.inner.control.lock().await;
        let Some(Some(details)) = control.peers.get_mut(&self.addr) else {
            return;
        };

        details.last_activity = now;
        details.last_global_activity = now;
        details.connected = connected;
    }

    #[track_caller]
    fn send(&self, msg: SyncResponse<A, D>) -> impl Future<Output = bool> + '_ {
        let caller = Location::caller();

        async {
            if self.write.send(msg).await.is_err() {
                tracing::warn!("failed to send response to peer {}:{}", caller.file(), caller.line());
                false
            } else {
                true
            }
        }
    }
}

async fn send_expect_ok<A, D>(
    write: &mpsc::Sender<SyncRequest<A, D>>,
    read: &mut mpsc::Receiver<SyncResponse<A, D>>,
    msg: SyncRequest<A, D>,
    timeout: Duration,
) -> bool
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    if write.send(msg).await.is_err() {
        return false;
    }

    matches!(recv_timeout(read, timeout).await, Some(SyncResponse::Ok))
}

async fn recv_timeout<A, D>(
    read: &mut mpsc::Receiver<SyncResponse<A, D>>,
    timeout: Duration,
) -> Option<SyncResponse<A, D>>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    tokio::time::timeout(timeout, read.recv()).await.ok().flatten()
}

fn generation_id<A: SyncIOAddress>(address: A) -> u64 {
    let mut hasher = DefaultHasher::new();
    address.hash(&mut hasher);
    hasher.finish() ^ now_ms().rotate_left(17) ^ PROMOTION_COUNTER.fetch_add(1, Ordering::SeqCst)
}

#[cfg(test)]
mod test;
