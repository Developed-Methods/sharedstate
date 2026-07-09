//! Drives the local deterministic state from the current leader address.

use std::{
    collections::{BTreeSet, HashMap},
    io::{Error, ErrorKind},
    iter,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use message_encoding::MessageEncoding;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    watch,
};

use crate::{
    cluster::node_state::NodeState,
    metrics::SharedStateMetrics,
    protocol::messages::{PROTOCOL_VERSION, SyncRequest, SyncResponse},
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::{RecoverableStateAction, RecoverableStateDetails},
        subscribable_state::StateHandle,
    },
    transport::{
        channels::NetIoSettings,
        traits::{SyncIO, SyncIOAddress},
    },
    utils::unique_state_id,
};

#[derive(Clone, Debug)]
pub struct StateSyncTiming {
    pub retry_delay: Duration,
    pub peer_failure_threshold: u32,
}

impl Default for StateSyncTiming {
    fn default() -> Self {
        Self {
            retry_delay: Duration::from_millis(500),
            peer_failure_threshold: 3,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct PeerHealth {
    consecutive_failures: u32,
    latency: Option<Duration>,
}

pub struct StateSyncTask<I: SyncIO, D: DeterministicState> {
    state: Arc<NodeState<I::Address, D>>,
    io: Arc<I>,
    settings: NetIoSettings,
    actions_rx: Receiver<D::Action>,
    handle: StateHandle<D>,
    leader_address: watch::Receiver<I::Address>,
    available_peers: watch::Receiver<Vec<I::Address>>,
    _leader_address_tx: watch::Sender<I::Address>,
    _available_peers_tx: watch::Sender<Vec<I::Address>>,
    peer_health: HashMap<I::Address, PeerHealth>,
    timing: StateSyncTiming,
    metrics: Arc<SharedStateMetrics>,
}

enum SyncFlow {
    Retry,
    RoleChanged,
    Shutdown,
}

impl<I, D> StateSyncTask<I, D>
where
    I: SyncIO,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    pub fn new(
        state: Arc<NodeState<I::Address, D>>,
        io: Arc<I>,
        settings: NetIoSettings,
        actions_rx: Receiver<D::Action>,
        leader_address_tx: watch::Sender<I::Address>,
        available_peers_tx: watch::Sender<Vec<I::Address>>,
        timing: StateSyncTiming,
        metrics: Arc<SharedStateMetrics>,
    ) -> Self {
        let handle = state.state.create_handle();
        let leader_address = state.leader_address.clone();
        let available_peers = state.available_peers.clone();
        Self {
            state,
            io,
            settings,
            actions_rx,
            handle,
            leader_address,
            available_peers,
            _leader_address_tx: leader_address_tx,
            _available_peers_tx: available_peers_tx,
            peer_health: HashMap::new(),
            timing,
            metrics,
        }
    }

    pub async fn run(mut self) {
        loop {
            if self.is_leader() {
                match self.lead().await {
                    SyncFlow::RoleChanged | SyncFlow::Retry => continue,
                    SyncFlow::Shutdown => return,
                }
            }

            self.state.set_connected_to_leader(false);
            match self.follow().await {
                SyncFlow::RoleChanged => continue,
                SyncFlow::Retry => {
                    tokio::select! {
                        _ = tokio::time::sleep(self.timing.retry_delay) => {}
                        changed = self.leader_address.changed() => {
                            changed.expect("leader address sender should be retained");
                        }
                        changed = self.available_peers.changed() => {
                            changed.expect("available peers sender should be retained");
                        }
                    }
                }
                SyncFlow::Shutdown => return,
            }
        }
    }

    async fn lead(&mut self) -> SyncFlow {
        let new_id = unique_state_id(&self.state.my_address);
        self.state
            .state
            .update(iter::once(RecoverableStateAction::BumpGeneration { new_id }))
            .await;
        self.state.set_connected_to_leader(true);
        tracing::info!("running as shared-state leader");

        loop {
            tokio::select! {
                action = self.actions_rx.recv() => {
                    let Some(action) = action else {
                        return SyncFlow::Shutdown;
                    };
                    let authority = self
                        .handle
                        .read_with(move |state| state.authority(RecoverableStateAction::StateAction { action }));
                    self.metrics.action_leader_count.inc();
                    self.state.state.update(iter::once(authority)).await;
                }
                changed = self.leader_address.changed() => {
                    changed.expect("leader address sender should be retained");
                    if !self.is_leader() {
                        tracing::info!(leader = ?self.current_leader(), "shared-state leadership revoked");
                        self.state.set_connected_to_leader(false);
                        return SyncFlow::RoleChanged;
                    }
                }
            }
        }
    }

    async fn follow(&mut self) -> SyncFlow {
        let peer = self.select_peer();
        let leader = self.current_leader();
        tracing::info!(?peer, ?leader, "connecting to shared-state sync peer");
        let connect = tokio::time::timeout(self.settings.message_timeout, self.io.connect(&peer));
        let connection = tokio::select! {
            result = connect => match result {
                Ok(Ok(connection)) => {
                    tracing::info!(?peer, ?leader, "connected to shared-state sync peer");
                    connection
                }
                Ok(Err(error)) => {
                    self.record_peer_failure(peer);
                    self.metrics.peer_connect_failure_count.inc();
                    tracing::info!(?peer, ?leader, ?error, "failed to connect to sync peer");
                    return SyncFlow::Retry;
                }
                Err(_) => {
                    self.record_peer_failure(peer);
                    self.metrics.peer_connect_timeout_count.inc();
                    tracing::info!(?peer, ?leader, timeout = ?self.settings.message_timeout, "timed out connecting to sync peer");
                    return SyncFlow::Retry;
                }
            },
            changed = self.leader_address.changed() => {
                changed.expect("leader address sender should be retained");
                if self.is_leader() {
                    tracing::info!("shared-state leadership granted");
                    return SyncFlow::RoleChanged;
                }
                return SyncFlow::Retry;
            }
        };
        self.metrics.peer_connect_success_count.inc();

        let (_remote, write, mut read) = connection.client_channels::<D>(self.settings.clone());
        let details = self.state.state.settled_recovery_details().await;
        tracing::info!(?peer, ?leader, local_next_seq = details.next_seq(), "starting shared-state peer subscription");
        let mut next_seq = match self.subscribe(peer, &write, &mut read, details).await {
            Ok((next_seq, latency)) => {
                self.record_peer_success(peer, latency);
                self.metrics.peer_subscription_success_count.inc();
                tracing::info!(?peer, ?leader, next_seq, latency = ?latency, "shared-state peer subscription succeeded");
                next_seq
            }
            Err(error) => {
                self.record_peer_failure(peer);
                self.metrics.peer_subscription_failure_count.inc();
                tracing::info!(?peer, ?leader, ?error, "sync peer subscription failed");
                return SyncFlow::Retry;
            }
        };

        self.state.set_connected_peer(Some(peer));
        self.state.set_connected_to_leader(true);
        tracing::info!(?peer, ?leader, next_seq, "subscribed to shared-state peer");

        loop {
            tokio::select! {
                response = read.recv() => match response {
                    Some(SyncResponse::Action { seq, action }) => {
                        if seq != next_seq {
                            self.record_peer_failure(peer);
                            self.state.set_connected_to_leader(false);
                            tracing::warn!(?peer, ?leader, seq, next_seq, "sync peer action stream out of sequence");
                            return SyncFlow::Retry;
                        }
                        next_seq += 1;
                        self.metrics.action_follower_count.inc();
                        self.state.state.update(iter::once(action)).await;
                    }
                    Some(response) => {
                        self.record_peer_failure(peer);
                        self.state.set_connected_to_leader(false);
                        tracing::warn!(?peer, ?leader, response = response.name(), "unexpected response from sync peer");
                        return SyncFlow::Retry;
                    }
                    None => {
                        self.record_peer_failure(peer);
                        self.state.set_connected_to_leader(false);
                        tracing::info!(?peer, ?leader, "sync peer subscription closed");
                        return SyncFlow::Retry;
                    }
                },
                action = self.actions_rx.recv() => {
                    let Some(action) = action else {
                        return SyncFlow::Shutdown;
                    };
                    if send(&write, SyncRequest::Action(action)).await.is_err() {
                        self.record_peer_failure(peer);
                        self.state.set_connected_to_leader(false);
                        tracing::warn!(?peer, ?leader, "failed to forward action to sync peer");
                        return SyncFlow::Retry;
                    }
                    self.metrics.action_forwarded_count.inc();
                }
                changed = self.leader_address.changed() => {
                    changed.expect("leader address sender should be retained");
                    if self.is_leader() {
                        tracing::info!("shared-state leadership granted");
                        self.state.set_connected_to_leader(false);
                        return SyncFlow::RoleChanged;
                    }
                }
                changed = self.available_peers.changed() => {
                    changed.expect("available peers sender should be retained");
                    let next_peer = self.select_peer();
                    if peer != next_peer {
                        tracing::debug!(?peer, ?next_peer, "sync peer selection changed");
                        self.state.set_connected_to_leader(false);
                        return SyncFlow::Retry;
                    }
                }
            }
        }
    }

    fn current_leader(&self) -> I::Address {
        *self.leader_address.borrow()
    }

    fn is_leader(&self) -> bool {
        self.state.my_address == self.current_leader()
    }

    fn select_peer(&self) -> I::Address {
        let leader = self.current_leader();
        let candidates = self.candidate_peers();
        if candidates.is_empty() {
            return leader;
        }

        let leader_failures = self
            .peer_health
            .get(&leader)
            .map(|health| health.consecutive_failures)
            .unwrap_or_default();

        if candidates.contains(&leader) && leader_failures < self.timing.peer_failure_threshold {
            return leader;
        }

        candidates
            .iter()
            .cloned()
            .min_by_key(|peer| self.peer_score(*peer))
            .unwrap()
    }

    fn candidate_peers(&self) -> Vec<I::Address> {
        let mut peers = BTreeSet::new();
        peers.insert(self.current_leader());
        for peer in self.available_peers.borrow().iter().copied() {
            peers.insert(peer);
        }
        peers.remove(&self.state.my_address);
        peers.into_iter().collect()
    }

    fn peer_score(&self, peer: I::Address) -> (u32, u128, I::Address) {
        let health = self.peer_health.get(&peer);
        let failures = health.map(|health| health.consecutive_failures).unwrap_or_default();
        let latency = health
            .and_then(|health| health.latency)
            .unwrap_or_else(|| self.settings.message_timeout / 2)
            .as_millis();
        (failures, latency, peer)
    }

    fn record_peer_success(&mut self, peer: I::Address, latency: Option<Duration>) {
        let health = self.peer_health.entry(peer).or_default();
        health.consecutive_failures = 0;
        if let Some(latency) = latency {
            health.latency = Some(latency);
        }
    }

    fn record_peer_failure(&mut self, peer: I::Address) {
        let health = self.peer_health.entry(peer).or_default();
        health.consecutive_failures = health.consecutive_failures.saturating_add(1);
    }

    async fn subscribe(
        &mut self,
        peer: I::Address,
        write: &Sender<SyncRequest<I::Address, D>>,
        read: &mut Receiver<SyncResponse<I::Address, D>>,
        details: RecoverableStateDetails,
    ) -> std::io::Result<(u64, Option<Duration>)> {
        tracing::info!(?peer, "starting sync peer protocol handshake");
        send(write, SyncRequest::ProtocolVersion(PROTOCOL_VERSION)).await?;
        expect_ok(recv(read, self.settings.message_timeout).await?, "protocol version")?;
        tracing::info!(?peer, "sync peer protocol handshake succeeded");

        let latency = if peer != self.current_leader() {
            Some(self.ping(write, read).await?)
        } else {
            None
        };

        let local_next_seq = details.next_seq();
        tracing::info!(?peer, local_next_seq, "requesting shared-state peer subscription");
        send(write, SyncRequest::Subscribe(details)).await?;

        match recv(read, self.settings.message_timeout).await? {
            SyncResponse::Ok => {
                self.metrics.record_incremental_recovery();
                tracing::info!(?peer, next_seq = local_next_seq, "shared-state peer accepted incremental subscription");
                Ok((local_next_seq, latency))
            }
            SyncResponse::FreshState(fresh) => {
                let next_seq = fresh.details().next_seq();
                tracing::info!(
                    ?peer,
                    local_next_seq,
                    fresh_next_seq = next_seq,
                    "sync peer sent fresh state; resetting local state"
                );
                self.state.state.reset(fresh).await;
                self.metrics.record_fresh_recovery();
                Ok((next_seq, latency))
            }
            SyncResponse::NotConnected => {
                tracing::info!(?peer, "sync peer rejected subscription because it is not connected");
                Err(Error::new(ErrorKind::NotConnected, "sync source is not connected"))
            }
            response => Err(unexpected("Ok or FreshState", &response)),
        }
    }

    async fn ping(
        &mut self,
        write: &Sender<SyncRequest<I::Address, D>>,
        read: &mut Receiver<SyncResponse<I::Address, D>>,
    ) -> std::io::Result<Duration> {
        let id = epoch_millis();
        send(write, SyncRequest::Ping(id)).await?;
        match recv(read, self.settings.message_timeout).await? {
            SyncResponse::Pong(pong_id) if pong_id == id => {
                Ok(Duration::from_millis(epoch_millis().saturating_sub(pong_id)))
            }
            SyncResponse::Pong(pong_id) => {
                Err(Error::new(ErrorKind::InvalidData, format!("expected Pong({id}), got Pong({pong_id})")))
            }
            response => Err(unexpected("Pong", &response)),
        }
    }
}

fn epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

async fn send<A: SyncIOAddress, D: DeterministicState>(
    write: &Sender<SyncRequest<A, D>>,
    request: SyncRequest<A, D>,
) -> std::io::Result<()> {
    write
        .send(request)
        .await
        .map_err(|error| Error::new(ErrorKind::BrokenPipe, format!("failed to send {:?}", error.0)))
}

async fn recv<A: SyncIOAddress, D: DeterministicState>(
    read: &mut Receiver<SyncResponse<A, D>>,
    timeout: Duration,
) -> std::io::Result<SyncResponse<A, D>> {
    match tokio::time::timeout(timeout, read.recv()).await {
        Ok(Some(response)) => Ok(response),
        Ok(None) => Err(Error::new(ErrorKind::UnexpectedEof, "connection closed")),
        Err(_) => Err(Error::new(ErrorKind::TimedOut, "timed out waiting for response")),
    }
}

fn expect_ok<A: SyncIOAddress, D: DeterministicState>(
    response: SyncResponse<A, D>,
    step: &'static str,
) -> std::io::Result<()> {
    match response {
        SyncResponse::Ok => Ok(()),
        response => {
            Err(Error::new(ErrorKind::InvalidData, format!("expected Ok during {step}, got {}", response.name())))
        }
    }
}

fn unexpected<A: SyncIOAddress, D: DeterministicState>(expected: &str, response: &SyncResponse<A, D>) -> Error {
    Error::new(ErrorKind::InvalidData, format!("expected {expected}, got {}", response.name()))
}
