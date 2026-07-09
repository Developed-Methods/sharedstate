//! Drives the local deterministic state from the current leader address.

use std::{
    collections::{BTreeSet, HashMap},
    io::{Error, ErrorKind},
    iter,
    sync::Arc,
    time::Duration,
};

use message_encoding::MessageEncoding;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    watch,
};

use crate::{
    cluster::node_state::NodeState,
    protocol::messages::{SyncRequest, SyncResponse, PROTOCOL_VERSION},
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::{RecoverableStateAction, RecoverableStateDetails},
        subscribable_state::StateHandle,
    },
    transport::{channels::NetIoSettings, traits::SyncIO},
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
    leader_updates_open: bool,
    peer_updates_open: bool,
    peer_health: HashMap<I::Address, PeerHealth>,
    timing: StateSyncTiming,
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
        timing: StateSyncTiming,
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
            leader_updates_open: true,
            peer_updates_open: true,
            peer_health: HashMap::new(),
            timing,
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
                    if self.leader_updates_open || self.peer_updates_open {
                        tokio::select! {
                            _ = tokio::time::sleep(self.timing.retry_delay) => {}
                            changed = self.leader_address.changed(), if self.leader_updates_open => {
                                if changed.is_err() {
                                    self.leader_updates_open = false;
                                }
                            }
                            changed = self.available_peers.changed(), if self.peer_updates_open => {
                                if changed.is_err() {
                                    self.peer_updates_open = false;
                                }
                            }
                        }
                    } else {
                        tokio::time::sleep(self.timing.retry_delay).await;
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
                    self.state.state.update(iter::once(authority)).await;
                }
                changed = self.leader_address.changed(), if self.leader_updates_open => {
                    if changed.is_err() {
                        self.leader_updates_open = false;
                    } else if !self.is_leader() {
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
        let connect = tokio::time::timeout(self.settings.message_timeout, self.io.connect(&peer));
        let connection = if self.leader_updates_open {
            tokio::select! {
                result = connect => match result {
                    Ok(Ok(connection)) => connection,
                    Ok(Err(error)) => {
                        self.record_peer_failure(peer);
                        tracing::debug!(?peer, ?leader, ?error, "failed to connect to sync peer");
                        return SyncFlow::Retry;
                    }
                    Err(_) => {
                        self.record_peer_failure(peer);
                        tracing::debug!(?peer, ?leader, "timed out connecting to sync peer");
                        return SyncFlow::Retry;
                    }
                },
                changed = self.leader_address.changed(), if self.leader_updates_open => {
                    if changed.is_err() {
                        self.leader_updates_open = false;
                    } else if self.is_leader() {
                        tracing::info!("shared-state leadership granted");
                        return SyncFlow::RoleChanged;
                    }
                    return SyncFlow::Retry;
                }
            }
        } else {
            match connect.await {
                Ok(Ok(connection)) => connection,
                Ok(Err(error)) => {
                    self.record_peer_failure(peer);
                    tracing::debug!(?peer, ?leader, ?error, "failed to connect to sync peer");
                    return SyncFlow::Retry;
                }
                Err(_) => {
                    self.record_peer_failure(peer);
                    tracing::debug!(?peer, ?leader, "timed out connecting to sync peer");
                    return SyncFlow::Retry;
                }
            }
        };

        let (_remote, write, mut read) = connection.client_channels::<D>(self.settings.clone());
        let details = self.state.state.settled_recovery_details().await;
        let mut next_seq = match self.subscribe(peer, &write, &mut read, details).await {
            Ok((next_seq, latency)) => {
                self.record_peer_success(peer, latency);
                next_seq
            }
            Err(error) => {
                self.record_peer_failure(peer);
                tracing::warn!(?peer, ?leader, ?error, "sync peer subscription failed");
                return SyncFlow::Retry;
            }
        };

        self.state.set_connected_to_leader(true);
        tracing::info!(?peer, ?leader, next_seq, "subscribed to shared-state peer");

        loop {
            tokio::select! {
                response = read.recv() => match response {
                    Some(SyncResponse::Action { seq, action }) => {
                        if seq != next_seq {
                            self.record_peer_failure(peer);
                            tracing::warn!(?peer, ?leader, seq, next_seq, "sync peer action stream out of sequence");
                            return SyncFlow::Retry;
                        }
                        next_seq += 1;
                        self.state.state.update(iter::once(action)).await;
                    }
                    Some(response) => {
                        self.record_peer_failure(peer);
                        tracing::warn!(?peer, ?leader, response = response.name(), "unexpected response from sync peer");
                        return SyncFlow::Retry;
                    }
                    None => {
                        self.record_peer_failure(peer);
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
                        tracing::warn!(?peer, ?leader, "failed to forward action to sync peer");
                        return SyncFlow::Retry;
                    }
                }
                changed = self.leader_address.changed(), if self.leader_updates_open => {
                    if changed.is_err() {
                        self.leader_updates_open = false;
                    } else if self.is_leader() {
                        tracing::info!("shared-state leadership granted");
                        return SyncFlow::RoleChanged;
                    }
                }
                changed = self.available_peers.changed(), if self.peer_updates_open => {
                    if changed.is_err() {
                        self.peer_updates_open = false;
                    } else {
                        let next_peer = self.select_peer();
                        if peer != next_peer {
                            tracing::debug!(?peer, ?next_peer, "sync peer selection changed");
                            return SyncFlow::Retry;
                        }
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
        let mut candidates = self.candidate_peers();
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

        candidates.sort_by_key(|peer| self.peer_score(*peer));
        candidates[0]
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
        write: &Sender<SyncRequest<D>>,
        read: &mut Receiver<SyncResponse<D>>,
        details: RecoverableStateDetails,
    ) -> std::io::Result<(u64, Option<Duration>)> {
        send(write, SyncRequest::ProtocolVersion(PROTOCOL_VERSION)).await?;
        expect_ok(recv(read, self.settings.message_timeout).await?, "protocol version")?;

        let latency = if peer != self.current_leader() {
            Some(self.ping(write, read).await?)
        } else {
            None
        };

        let local_next_seq = details.next_seq();
        send(write, SyncRequest::Subscribe(details)).await?;

        match recv(read, self.settings.message_timeout).await? {
            SyncResponse::Ok => Ok((local_next_seq, latency)),
            SyncResponse::FreshState(fresh) => {
                let next_seq = fresh.details().next_seq();
                self.state.state.reset(fresh).await;
                Ok((next_seq, latency))
            }
            SyncResponse::NotConnected => Err(Error::new(ErrorKind::NotConnected, "sync source is not connected")),
            response => Err(unexpected("Ok or FreshState", &response)),
        }
    }

    async fn ping(
        &mut self,
        write: &Sender<SyncRequest<D>>,
        read: &mut Receiver<SyncResponse<D>>,
    ) -> std::io::Result<Duration> {
        let start = tokio::time::Instant::now();
        send(write, SyncRequest::Ping).await?;
        match recv(read, self.settings.message_timeout).await? {
            SyncResponse::Pong => Ok(start.elapsed()),
            response => Err(unexpected("Pong", &response)),
        }
    }
}

async fn send<D: DeterministicState>(write: &Sender<SyncRequest<D>>, request: SyncRequest<D>) -> std::io::Result<()> {
    write
        .send(request)
        .await
        .map_err(|error| Error::new(ErrorKind::BrokenPipe, format!("failed to send {:?}", error.0)))
}

async fn recv<D: DeterministicState>(
    read: &mut Receiver<SyncResponse<D>>,
    timeout: Duration,
) -> std::io::Result<SyncResponse<D>> {
    match tokio::time::timeout(timeout, read.recv()).await {
        Ok(Some(response)) => Ok(response),
        Ok(None) => Err(Error::new(ErrorKind::UnexpectedEof, "connection closed")),
        Err(_) => Err(Error::new(ErrorKind::TimedOut, "timed out waiting for response")),
    }
}

fn expect_ok<D: DeterministicState>(response: SyncResponse<D>, step: &'static str) -> std::io::Result<()> {
    match response {
        SyncResponse::Ok => Ok(()),
        response => {
            Err(Error::new(ErrorKind::InvalidData, format!("expected Ok during {step}, got {}", response.name())))
        }
    }
}

fn unexpected<D: DeterministicState>(expected: &str, response: &SyncResponse<D>) -> Error {
    Error::new(ErrorKind::InvalidData, format!("expected {expected}, got {}", response.name()))
}
