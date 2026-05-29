use std::{
    collections::hash_map::DefaultHasher,
    collections::{hash_map, BTreeSet, HashMap, HashSet},
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

use message_encoding::MessageEncoding;
use tokio::{
    sync::{broadcast, mpsc, Mutex},
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
        messages::{
            ElectionObservation, LeaderInfoMessage, LeaderStatus, SharePeerDetails, SyncRequest,
            SyncResponse,
        },
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
const OBSERVATION_STALE_MS: u64 = 15_000;
const OBSERVATION_INTERVAL: Duration = Duration::from_secs(3);
const FOLLOW_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const RPC_TIMEOUT: Duration = Duration::from_secs(5);

pub struct NodeState<A: SyncIOAddress, D: DeterministicState> {
    inner: Arc<Inner<A, D>>,
    io_settings: NetIoSettings,
}

struct Inner<A: SyncIOAddress, D: DeterministicState> {
    address: A,
    can_lead: bool,
    leader: Mutex<LeaderInfo<A>>,
    peers: Mutex<HashMap<A, Option<PeerDetails<A>>>>,
    state: Mutex<AuthorativeState<D>>,
    state_reader: SharedStateReader<RecoverableState<D>>,
    actions_tx: broadcast::Sender<(A, D::Action)>,
    follow: Mutex<Option<FollowConnection<A, D>>>,
    election: Mutex<ElectionState<A>>,
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

struct ElectionState<A: SyncIOAddress> {
    term: u64,
    known_can_lead: BTreeSet<A>,
    observations: HashMap<A, ElectionObservation<A>>,
    last_promoted_leader: Option<A>,
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
    pub async fn new(
        address: A,
        init_state: RecoverableState<D>,
        can_lead: bool,
        io_settings: NetIoSettings,
    ) -> Self {
        let (actions_tx, _) = broadcast::channel(2048);
        let state = AuthorativeState::new(init_state).await;
        let state_reader = state.state_reader();

        NodeState {
            inner: Arc::new(Inner {
                address,
                can_lead,
                leader: Mutex::new(LeaderInfo {
                    leader: None,
                    path: None,
                    term: 0,
                }),
                peers: Mutex::new({
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
                }),
                state: Mutex::new(state),
                state_reader,
                actions_tx,
                follow: Mutex::new(None),
                election: Mutex::new(ElectionState {
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
                }),
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

    pub async fn start_client<I>(&self, io: Arc<I>) -> JoinHandle<()>
    where
        I: SyncIO<Address = A>,
        D: MessageEncoding,
        D::Action: MessageEncoding + Clone,
        D::AuthorityAction: MessageEncoding,
    {
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

    pub async fn handle_client<I: SyncIO<Address = A>>(
        &self,
        conn: SyncConnection<I>,
    ) -> JoinHandle<()>
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
    pub async fn discover_peers(
        &self,
        peers: impl Iterator<Item = impl Into<SharePeerDetails<A>>>,
    ) {
        let mut peers_lock = self.peers.lock().await;
        let mut election = self.election.lock().await;

        for peer in peers {
            let details = peer.into();

            if details.address == self.address {
                continue;
            }

            match peers_lock.entry(details.address) {
                hash_map::Entry::Vacant(v) => {
                    v.insert(details.can_be_leader.map(|can_lead| {
                        if can_lead {
                            election.known_can_lead.insert(details.address);
                        }

                        PeerDetails {
                            last_activity: None,
                            last_connect_attempt: None,
                            last_connect_fail: None,
                            repeat_connect_fails: 0,
                            latency_ms: None,
                            can_lead,
                            last_global_activity: details.last_global_activity,
                            connected: false,
                            last_observation: None,
                        }
                    }));
                }
                hash_map::Entry::Occupied(o) => {
                    let value = o.into_mut();

                    if let Some(can_lead) = details.can_be_leader {
                        if can_lead {
                            election.known_can_lead.insert(details.address);
                        } else {
                            election.known_can_lead.remove(&details.address);
                        }

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
        let locked = self.peers.lock().await;

        locked
            .iter()
            .map(|(address, details)| SharePeerDetails {
                address: *address,
                can_be_leader: details.as_ref().map(|v| v.can_lead),
                last_global_activity: details.as_ref().and_then(|v| v.last_global_activity),
            })
            .collect::<Vec<_>>()
    }

    async fn leader_info_message(&self) -> LeaderInfoMessage<A> {
        let leader = self.leader.lock().await.clone();
        let follow_path = self
            .follow
            .lock()
            .await
            .as_ref()
            .map(|follow| follow.leader_path.clone());
        LeaderInfoMessage {
            leader: leader.leader,
            path: leader.path.or(follow_path),
            term: leader.term,
        }
    }

    async fn local_observation(&self) -> ElectionObservation<A> {
        let leader = self.leader.lock().await.clone();
        let reachable_can_lead = {
            let peers = self.peers.lock().await;
            peers
                .iter()
                .filter_map(|(addr, details)| {
                    let details = details.as_ref()?;
                    if details.can_lead && (details.connected || *addr == self.address) {
                        Some(*addr)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };

        ElectionObservation {
            observer: self.address,
            term: leader.term,
            leader: leader.leader,
            leader_path: leader.path,
            can_lead: self.can_lead,
            reachable_can_lead,
        }
    }

    async fn record_observation(&self, observation: ElectionObservation<A>) {
        if observation.can_lead {
            self.election
                .lock()
                .await
                .known_can_lead
                .insert(observation.observer);
        }

        self.election
            .lock()
            .await
            .observations
            .insert(observation.observer, observation.clone());

        let mut peers = self.peers.lock().await;
        let details = peers
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
        let (known_can_lead, observations, local_term) = {
            let election = self.election.lock().await;
            (
                election.known_can_lead.clone(),
                election.observations.clone(),
                election.term,
            )
        };
        let peer_details = self.peers.lock().await.clone();

        let mut counts: HashMap<(u64, A), u64> = HashMap::new();
        let mut valid_leaders: HashMap<A, (u64, Option<Vec<A>>)> = HashMap::new();

        for observation in std::iter::once(local_observation).chain(observations.into_values()) {
            if observation.observer != self.address {
                let Some(Some(details)) = peer_details.get(&observation.observer) else {
                    continue;
                };
                let Some(last_activity) = details.last_activity else {
                    continue;
                };
                if OBSERVATION_STALE_MS < now.saturating_sub(last_activity.get()) {
                    continue;
                }
            }

            let Some(leader) = observation.leader else {
                continue;
            };
            if let Some(path) = &observation.leader_path {
                if !valid_remote_leader_path(Some(leader), path, self.address) {
                    continue;
                }
            }

            *counts.entry((observation.term, leader)).or_insert(0) += 1;
            valid_leaders
                .entry(leader)
                .and_modify(|existing| {
                    if existing.0 < observation.term {
                        *existing = (observation.term, observation.leader_path.clone());
                    }
                })
                .or_insert((observation.term, observation.leader_path.clone()));
        }

        let mut selected = counts
            .into_iter()
            .max_by(
                |((a_term, a_leader), a_count), ((b_term, b_leader), b_count)| {
                    a_term
                        .cmp(b_term)
                        .then(a_count.cmp(b_count))
                        .then_with(|| b_leader.cmp(a_leader))
                },
            )
            .map(|((term, leader), _)| (term, leader));

        if selected.is_none() {
            selected = known_can_lead
                .iter()
                .filter(|addr| {
                    **addr == self.address
                        || peer_details
                            .get(addr)
                            .and_then(|d| d.as_ref())
                            .map(|d| d.connected || d.last_activity.is_some())
                            .unwrap_or(false)
                })
                .next()
                .map(|leader| (local_term, *leader));
        }

        let Some((term, leader)) = selected else {
            return;
        };

        let known_count = known_can_lead.len().max(usize::from(self.can_lead));
        let majority = known_count / 2 + 1;
        let reachable_count = known_can_lead
            .iter()
            .filter(|addr| {
                **addr == self.address
                    || peer_details
                        .get(addr)
                        .and_then(|d| d.as_ref())
                        .and_then(|d| d.last_activity)
                        .map(|ts| now.saturating_sub(ts.get()) <= OBSERVATION_STALE_MS)
                        .unwrap_or(false)
            })
            .count();

        if leader == self.address && self.can_lead && majority <= reachable_count {
            self.promote_if_needed(term).await;
            return;
        }

        if leader != self.address {
            let path = valid_leaders
                .get(&leader)
                .and_then(|(_, path)| path.clone());
            let mut lock = self.leader.lock().await;
            if lock.term < term || lock.leader != Some(leader) {
                lock.term = term;
                lock.leader = Some(leader);
                lock.path = path;
            }
        }
    }

    async fn promote_if_needed(&self, observed_term: u64) {
        {
            let leader = self.leader.lock().await;
            if leader.leader == Some(self.address)
                && leader.path.as_deref() == Some(&[self.address])
            {
                return;
            }
        }

        let new_term = {
            let mut election = self.election.lock().await;
            election.term = election.term.max(observed_term).saturating_add(1);
            election.last_promoted_leader = Some(self.address);
            election.term
        };

        {
            let mut leader = self.leader.lock().await;
            leader.leader = Some(self.address);
            leader.path = Some(vec![self.address]);
            leader.term = new_term;
        }

        let new_id = generation_id(self.address);
        let mut state = self.state.lock().await;
        state
            .apply_authority(RecoverableStateAction::BumpGeneration { new_id })
            .await;
    }

    async fn clear_follow(&self) {
        if let Some(follow) = self.follow.lock().await.take() {
            follow.cancel.cancel();
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
            self.observe_can_lead_peers().await;
            self.inner.apply_election().await;
            self.ensure_follow_connection().await;
            tokio::time::sleep(OBSERVATION_INTERVAL).await;
        }
    }

    async fn observe_can_lead_peers(&self) {
        let targets = {
            let peers = self.inner.peers.lock().await;
            peers
                .iter()
                .filter_map(|(addr, details)| {
                    if *addr == self.inner.address {
                        return None;
                    }
                    match details {
                        Some(details) if details.can_lead => Some(*addr),
                        None => Some(*addr),
                        _ => None,
                    }
                })
                .collect::<Vec<_>>()
        };

        for target in targets {
            self.observe_peer(target).await;
        }
    }

    async fn observe_peer(&self, target: I::Address) {
        self.mark_connect_attempt(target).await;

        let started = now_ms();
        let Ok(Ok(conn)) = tokio::time::timeout(RPC_TIMEOUT, self.io.connect(&target)).await else {
            self.mark_connect_fail(target).await;
            return;
        };

        let (_addr, write, mut read) = conn.client_channels::<D>(self.io_settings.clone());
        if !send_expect_ok(
            &write,
            &mut read,
            SyncRequest::ProtocolVersion(PROTOCOL_VERSION),
        )
        .await
        {
            self.mark_connect_fail(target).await;
            return;
        }
        if !send_expect_ok(
            &write,
            &mut read,
            SyncRequest::MyAddress(self.inner.address),
        )
        .await
        {
            self.mark_connect_fail(target).await;
            return;
        }
        if write.send(SyncRequest::Ping(started)).await.is_err() {
            self.mark_connect_fail(target).await;
            return;
        }
        match recv_timeout(&mut read).await {
            Some(SyncResponse::Pong(id)) if id == started => {
                self.mark_connected(target, now_ms().saturating_sub(started))
                    .await;
            }
            _ => {
                self.mark_connect_fail(target).await;
                return;
            }
        }

        let peers = self.inner.peer_snapshot().await;
        if write.send(SyncRequest::SharePeers(peers)).await.is_ok() {
            if let Some(SyncResponse::Peers(peers)) = recv_timeout(&mut read).await {
                self.inner.discover_peers(peers.into_iter()).await;
            }
        }

        if write.send(SyncRequest::ShareElection).await.is_ok() {
            if let Some(SyncResponse::Election(observation)) = recv_timeout(&mut read).await {
                self.inner.record_observation(observation).await;
            }
        }
    }

    async fn ensure_follow_connection(&self) {
        let leader = self.inner.leader.lock().await.leader;
        if leader == Some(self.inner.address) {
            self.inner.clear_follow().await;
            return;
        }

        if self.inner.follow.lock().await.is_some() {
            return;
        }

        let Some(target) = self.select_follow_target(leader).await else {
            tokio::time::sleep(FOLLOW_RETRY_INTERVAL).await;
            return;
        };

        self.connect_follow(target, leader).await;
    }

    async fn select_follow_target(&self, leader: Option<I::Address>) -> Option<I::Address> {
        let peers = self.inner.peers.lock().await;
        let mut candidates = peers
            .iter()
            .filter_map(|(addr, details)| {
                if *addr == self.inner.address {
                    return None;
                }
                Some((*addr, details.as_ref()?))
            })
            .collect::<Vec<_>>();

        candidates.sort_by_key(|(addr, details)| {
            let tier = if Some(*addr) == leader {
                0u8
            } else if leader.is_some()
                && details.last_observation.as_ref().and_then(|o| o.leader) == leader
            {
                1
            } else if details.can_lead {
                2
            } else {
                3
            };
            (
                tier,
                !details.connected,
                details.latency_ms.unwrap_or(u64::MAX),
                details.repeat_connect_fails,
                details.last_connect_fail.map(|v| v.get()).unwrap_or(0),
                *addr,
            )
        });

        candidates.first().map(|(addr, _)| *addr)
    }

    async fn connect_follow(&self, target: I::Address, selected_leader: Option<I::Address>) {
        self.mark_connect_attempt(target).await;

        let Ok(Ok(conn)) = tokio::time::timeout(RPC_TIMEOUT, self.io.connect(&target)).await else {
            self.mark_connect_fail(target).await;
            return;
        };

        let (remote, write, mut read) = conn.client_channels::<D>(self.io_settings.clone());
        if !send_expect_ok(
            &write,
            &mut read,
            SyncRequest::ProtocolVersion(PROTOCOL_VERSION),
        )
        .await
        {
            self.mark_connect_fail(target).await;
            return;
        }
        if !send_expect_ok(
            &write,
            &mut read,
            SyncRequest::MyAddress(self.inner.address),
        )
        .await
        {
            self.mark_connect_fail(target).await;
            return;
        }

        let leader_info = if write.send(SyncRequest::WhoIsLeader).await.is_ok() {
            match recv_timeout(&mut read).await {
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

        {
            let mut leader_lock = self.inner.leader.lock().await;
            if leader_lock.term <= leader_info.term {
                leader_lock.term = leader_info.term;
                leader_lock.leader = Some(leader);
                leader_lock.path = Some(append_path(path.clone(), self.inner.address));
            }
        }

        let details = self
            .inner
            .state
            .lock()
            .await
            .recoverable_state_details()
            .await;
        if write
            .send(SyncRequest::SubscribeRecovery(details))
            .await
            .is_err()
        {
            self.mark_connect_fail(target).await;
            return;
        }

        let first_state_msg = match recv_timeout(&mut read).await {
            Some(SyncResponse::Accepted(_seq)) => None,
            Some(SyncResponse::RecoveryFailed(_)) => {
                if write.send(SyncRequest::SubscribeFresh).await.is_err() {
                    self.mark_connect_fail(target).await;
                    return;
                }
                match recv_timeout(&mut read).await {
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
            leader_path: append_path(path, self.inner.address),
            to_peer: write.clone(),
            cancel: cancel.clone(),
        };

        if let Some(existing) = self.inner.follow.lock().await.replace(follow) {
            existing.cancel.cancel();
        }

        self.mark_connected(target, 0).await;
        self.spawn_action_forward(write.clone(), cancel.clone());
        self.spawn_follow_reader(target, read, cancel);
    }

    fn spawn_action_forward(
        &self,
        write: mpsc::Sender<SyncRequest<I::Address, D>>,
        cancel: CancellationToken,
    ) {
        let mut actions = self.inner.actions_tx.subscribe();

        tokio::spawn(async move {
            loop {
                let recv = tokio::select! {
                    _ = cancel.cancelled() => break,
                    recv = actions.recv() => recv,
                };
                let Ok((source, action)) = recv else {
                    break;
                };

                if write
                    .send(SyncRequest::Action { source, action })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
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
                    Some(SyncResponse::LeaderInfo(info)) => {
                        let mut leader = inner.leader.lock().await;
                        if leader.term <= info.term {
                            leader.term = info.term;
                            leader.leader = info.leader;
                            leader.path = info.path;
                        }
                    }
                    Some(SyncResponse::LeaderPath(path)) => {
                        if let Some(leader_addr) = path.first().copied() {
                            if valid_remote_leader_path(Some(leader_addr), &path, inner.address) {
                                inner.leader.lock().await.path =
                                    Some(append_path(path, inner.address));
                            }
                        }
                    }
                    Some(SyncResponse::Peers(peers)) => {
                        inner.discover_peers(peers.into_iter()).await;
                    }
                    Some(SyncResponse::ActionStreamClosed) | None => break,
                    _ => {}
                }
            }

            if let Some(follow) = inner.follow.lock().await.take() {
                if follow.remote == target {
                    follow.cancel.cancel();
                } else {
                    let _ = inner.follow.lock().await.replace(follow);
                }
            }
        });
    }

    async fn mark_connect_attempt(&self, target: I::Address) {
        let mut peers = self.inner.peers.lock().await;
        if let Some(Some(details)) = peers.get_mut(&target) {
            details.last_connect_attempt = NonZeroU64::new(now_ms());
        }
    }

    async fn mark_connect_fail(&self, target: I::Address) {
        let mut peers = self.inner.peers.lock().await;
        if let Some(Some(details)) = peers.get_mut(&target) {
            details.last_connect_fail = NonZeroU64::new(now_ms());
            details.repeat_connect_fails = details.repeat_connect_fails.saturating_add(1);
            details.connected = false;
        }
    }

    async fn mark_connected(&self, target: I::Address, latency_ms: u64) {
        let mut peers = self.inner.peers.lock().await;
        if let Some(Some(details)) = peers.get_mut(&target) {
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
                    if !self
                        .send(SyncResponse::Peers(self.inner.peer_snapshot().await))
                        .await
                    {
                        break;
                    }
                }
                SyncRequest::WhoIsLeader => {
                    if !self
                        .send(SyncResponse::LeaderInfo(
                            self.inner.leader_info_message().await,
                        ))
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
                    let path = self.inner.leader.lock().await.path.clone();
                    let resp = match path {
                        None => SyncResponse::NoPathToLeader,
                        Some(path)
                            if valid_local_leader_path(
                                path.first().copied(),
                                &path,
                                self.inner.address,
                            ) =>
                        {
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

        let write = self.write.clone();
        tokio::spawn(async move {
            while let Ok((seq, action)) = feed.recv().await {
                if write
                    .send(SyncResponse::AuthorityAction(seq, action))
                    .await
                    .is_err()
                {
                    break;
                }
            }

            let _ = write.send(SyncResponse::ActionStreamClosed).await;
        });

        true
    }

    async fn subscribe_recovery(
        &self,
        details: crate::state::recoverable_state::RecoverableStateDetails,
    ) -> bool {
        let leader_state = self
            .inner
            .state
            .lock()
            .await
            .recoverable_state_details()
            .await;

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

        let write = self.write.clone();
        tokio::spawn(async move {
            while let Ok((seq, action)) = feed.recv().await {
                if write
                    .send(SyncResponse::AuthorityAction(seq, action))
                    .await
                    .is_err()
                {
                    break;
                }
            }

            let _ = write.send(SyncResponse::ActionStreamClosed).await;
        });

        true
    }

    async fn handle_action(&self, source: A, action: D::Action) {
        let is_leader = self.inner.leader.lock().await.leader == Some(self.inner.address);
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

        if let Some(follow) = self.inner.follow.lock().await.as_ref() {
            if follow
                .to_peer
                .send(SyncRequest::Action { source, action })
                .await
                .is_err()
            {
                tracing::warn!("failed to forward action to upstream leader");
            }
            return;
        }

        let _ = self.inner.actions_tx.send((source, action));
    }

    async fn handle_leader_status(&self, address: A, status: LeaderStatus<A>) {
        match status {
            LeaderStatus::Promoted { term, leader } => {
                let mut lock = self.inner.leader.lock().await;
                if lock.term < term || (lock.term == term && Some(leader) < lock.leader) {
                    lock.term = term;
                    lock.leader = Some(leader);
                    lock.path = if leader == self.inner.address {
                        Some(vec![self.inner.address])
                    } else if leader == address {
                        Some(vec![leader, self.inner.address])
                    } else {
                        None
                    };
                }
            }
            LeaderStatus::Offline { term, leader } => {
                let mut lock = self.inner.leader.lock().await;
                if lock.term <= term && lock.leader == Some(leader) {
                    lock.leader = None;
                    lock.path = None;
                    lock.term = term;
                }
            }
            LeaderStatus::Observation(observation) => {
                self.inner.record_observation(observation).await;
            }
        }
    }

    async fn record_activity_ts(&self, connected: bool) {
        let now = NonZeroU64::new(now_ms());

        let mut lock = self.inner.peers.lock().await;
        let Some(Some(details)) = lock.get_mut(&self.addr) else {
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
                tracing::warn!(
                    "failed to send response to peer {}:{}",
                    caller.file(),
                    caller.line()
                );
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
) -> bool
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    if write.send(msg).await.is_err() {
        return false;
    }

    matches!(recv_timeout(read).await, Some(SyncResponse::Ok))
}

async fn recv_timeout<A, D>(
    read: &mut mpsc::Receiver<SyncResponse<A, D>>,
) -> Option<SyncResponse<A, D>>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    tokio::time::timeout(RPC_TIMEOUT, read.recv())
        .await
        .ok()
        .flatten()
}

fn valid_remote_leader_path<A: SyncIOAddress>(leader: Option<A>, path: &[A], local: A) -> bool {
    let Some(leader) = leader else {
        return false;
    };
    if path.is_empty() || path[0] != leader {
        return false;
    }

    let mut seen = HashSet::new();
    for item in path {
        if !seen.insert(*item) {
            return false;
        }
    }

    !path.iter().skip(1).any(|step| *step == local)
}

fn valid_local_leader_path<A: SyncIOAddress>(leader: Option<A>, path: &[A], local: A) -> bool {
    let Some(leader) = leader else {
        return false;
    };
    if path.is_empty() || path[0] != leader {
        return false;
    }

    let mut seen = HashSet::new();
    for item in path {
        if !seen.insert(*item) {
            return false;
        }
    }

    path.last().copied() == Some(local)
}

fn append_path<A: SyncIOAddress>(mut path: Vec<A>, local: A) -> Vec<A> {
    if !path.contains(&local) {
        path.push(local);
    }
    path
}

fn generation_id<A: SyncIOAddress>(address: A) -> u64 {
    let mut hasher = DefaultHasher::new();
    address.hash(&mut hasher);
    hasher.finish() ^ now_ms().rotate_left(17) ^ PROMOTION_COUNTER.fetch_add(1, Ordering::SeqCst)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::message_channel::NetIoSettings;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestState {
        seq: u64,
        value: u64,
    }

    impl DeterministicState for TestState {
        type Action = u64;
        type AuthorityAction = u64;

        fn accept_seq(&self) -> u64 {
            self.seq
        }

        fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
            action
        }

        fn update(&mut self, action: &Self::AuthorityAction) {
            self.value += *action;
            self.seq += 1;
        }
    }

    impl MessageEncoding for TestState {
        fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
            Ok(self.seq.write_to(out)? + self.value.write_to(out)?)
        }

        fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
            Ok(Self {
                seq: MessageEncoding::read_from(read)?,
                value: MessageEncoding::read_from(read)?,
            })
        }
    }

    async fn node(address: u64, can_lead: bool) -> NodeState<u64, TestState> {
        NodeState::new(
            address,
            RecoverableState::new(address, TestState { seq: 1, value: 0 }),
            can_lead,
            NetIoSettings::default(),
        )
        .await
    }

    #[test]
    fn remote_leader_path_rejects_cycles_and_local_node() {
        assert!(valid_remote_leader_path(Some(1), &[1, 2], 3));
        assert!(!valid_remote_leader_path(Some(1), &[1, 2, 3], 3));
        assert!(!valid_remote_leader_path(Some(1), &[1, 2, 2], 3));
        assert!(!valid_remote_leader_path(Some(2), &[1, 2], 3));
    }

    #[test]
    fn local_leader_path_must_end_at_local_node() {
        assert!(valid_local_leader_path(Some(1), &[1, 2, 3], 3));
        assert!(!valid_local_leader_path(Some(1), &[1, 2], 3));
        assert!(!valid_local_leader_path(Some(1), &[1, 2, 2], 2));
    }

    #[tokio::test]
    async fn can_lead_node_does_not_promote_without_majority() {
        let node = node(2, true).await;

        node.inner
            .discover_peers(
                [
                    SharePeerDetails {
                        address: 1,
                        can_be_leader: Some(true),
                        last_global_activity: None,
                    },
                    SharePeerDetails {
                        address: 3,
                        can_be_leader: Some(true),
                        last_global_activity: None,
                    },
                ]
                .into_iter(),
            )
            .await;

        node.inner.apply_election().await;

        let leader = node.inner.leader.lock().await;
        assert_eq!(leader.leader, None);
        assert_eq!(leader.path, None);
    }

    #[tokio::test]
    async fn can_lead_node_promotes_with_majority_reachability() {
        let node = node(2, true).await;

        node.inner
            .discover_peers(
                [
                    SharePeerDetails {
                        address: 1,
                        can_be_leader: Some(true),
                        last_global_activity: None,
                    },
                    SharePeerDetails {
                        address: 3,
                        can_be_leader: Some(true),
                        last_global_activity: None,
                    },
                ]
                .into_iter(),
            )
            .await;

        {
            let mut peers = node.inner.peers.lock().await;
            let peer = peers.get_mut(&3).unwrap().as_mut().unwrap();
            peer.last_activity = NonZeroU64::new(now_ms());
            peer.connected = true;
        }

        node.inner.apply_election().await;

        let leader = node.inner.leader.lock().await;
        assert_eq!(leader.leader, Some(2));
        assert_eq!(leader.path, Some(vec![2]));
        assert!(0 < leader.term);
    }
}
