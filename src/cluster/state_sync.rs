//! Drives the local deterministic state based on the node's leader state.
//!
//! When leading, client actions are made authoritative, applied to the state,
//! and pushed into the sequenced broadcast for subscribers. When following,
//! the task subscribes to the leader's action feed (recovering the existing
//! state when possible, resetting from a fresh snapshot otherwise) and
//! forwards client actions to the leader. If the leader cannot be reached
//! directly, the task relays through another peer after confirming over RPC
//! that the peer follows the same leader.

use std::{
    io::{Error, ErrorKind},
    iter,
    sync::Arc,
    time::Duration,
};

use message_encoding::MessageEncoding;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    cluster::{node_state::NodeState, peer_connections::PeerConnections},
    protocol::messages::{ElectionTerm, LeaderMode, PROTOCOL_VERSION, SyncRequest, SyncResponse},
    state::{
        deterministic_state::DeterministicState, recoverable_state::RecoverableStateAction,
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
    /// How often the task re-checks the node's leader state while working.
    pub leader_poll_interval: Duration,
    /// How long to wait before retrying when no sync source is available.
    pub retry_delay: Duration,
}

impl Default for StateSyncTiming {
    fn default() -> Self {
        Self {
            leader_poll_interval: Duration::from_millis(100),
            retry_delay: Duration::from_millis(500),
        }
    }
}

pub struct StateSyncTask<I: SyncIO, D: DeterministicState> {
    state: Arc<NodeState<I::Address, D>>,
    peer_connections: Arc<PeerConnections<I, D>>,
    io: Arc<I>,
    settings: NetIoSettings,
    actions_rx: Receiver<(I::Address, D::Action)>,
    handle: StateHandle<D>,
    timing: StateSyncTiming,
}

enum Flow {
    Continue,
    Shutdown,
}

enum SyncAttempt {
    /// Could not establish a subscription with the target.
    Unreachable,
    /// A subscription ran and ended; the leader state should be re-checked.
    Finished,
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
        peer_connections: Arc<PeerConnections<I, D>>,
        io: Arc<I>,
        settings: NetIoSettings,
        actions_rx: Receiver<(I::Address, D::Action)>,
        timing: StateSyncTiming,
    ) -> Self {
        let handle = state.state.create_handle();
        Self {
            state,
            peer_connections,
            io,
            settings,
            actions_rx,
            handle,
            timing,
        }
    }

    pub async fn run(mut self) {
        loop {
            let leader_state = self.state.leader_state.lock().await.clone();
            let flow = match leader_state.mode {
                LeaderMode::Leading => self.lead(leader_state.term).await,
                LeaderMode::Following { leader } => self.follow(leader).await,
                LeaderMode::NoLeader | LeaderMode::Electing { .. } => self.wait_for_leader().await,
            };

            if matches!(flow, Flow::Shutdown) {
                tracing::info!("action channel closed, stopping state sync task");
                return;
            }
        }
    }

    /// Waits for a leader to emerge. Queued actions stay in the channel so
    /// they can be processed once a sync source is available.
    async fn wait_for_leader(&mut self) -> Flow {
        tokio::time::sleep(self.timing.leader_poll_interval).await;
        Flow::Continue
    }

    async fn lead(&mut self, term: ElectionTerm) -> Flow {
        /* bump the recovery generation so followers of the previous leader
         * can recover from us without a full state transfer */
        let new_id = unique_state_id(&self.state.my_address);
        self.state
            .state
            .update(iter::once(RecoverableStateAction::BumpGeneration { new_id }))
            .await;
        tracing::info!(%term, "leading, taking authority over shared state");

        loop {
            tokio::select! {
                action = self.actions_rx.recv() => {
                    let Some((source, action)) = action else {
                        return Flow::Shutdown;
                    };

                    let current = self.state.leader_state.lock().await.mode.clone();
                    if let LeaderMode::Following { leader } = current {
                        tracing::info!(?leader, "no longer leading, forwarding queued action to new leader");
                        self.forward_action(leader, source, action).await;
                        return Flow::Continue;
                    }
                    if !matches!(current, LeaderMode::Leading) {
                        tracing::info!("no longer leading, releasing authority before applying queued action");
                        return Flow::Continue;
                    }

                    let authority = self
                        .handle
                        .read_with(move |state| state.authority(RecoverableStateAction::StateAction { action }));
                    self.state.state.update(iter::once(authority)).await;
                    tracing::debug!(?source, "applied action with local authority");
                }
                _ = tokio::time::sleep(self.timing.leader_poll_interval) => {
                    if !matches!(self.state.leader_state.lock().await.mode, LeaderMode::Leading) {
                        tracing::info!(%term, "no longer leading, releasing authority");
                        return Flow::Continue;
                    }
                }
            }
        }
    }

    async fn follow(&mut self, leader: I::Address) -> Flow {
        match self.sync_from(leader, leader).await {
            SyncAttempt::Finished => return Flow::Continue,
            SyncAttempt::Shutdown => return Flow::Shutdown,
            SyncAttempt::Unreachable => {}
        }

        tracing::warn!(?leader, "cannot subscribe to leader directly, looking for a relay peer");

        for relay in self.relay_candidates(leader).await {
            match self.peer_connections.query_leader(relay).await {
                Ok(state) if matches!(&state.mode, LeaderMode::Following { leader: relayed } if *relayed == leader) => {}
                Ok(state) => {
                    tracing::debug!(?relay, ?state, "relay candidate does not follow our leader, skipping");
                    continue;
                }
                Err(error) => {
                    tracing::debug!(?relay, ?error, "failed to query relay candidate for its leader");
                    continue;
                }
            }

            tracing::info!(?relay, ?leader, "syncing state through relay peer");
            match self.sync_from(relay, leader).await {
                SyncAttempt::Finished => return Flow::Continue,
                SyncAttempt::Shutdown => return Flow::Shutdown,
                SyncAttempt::Unreachable => continue,
            }
        }

        tracing::warn!(?leader, "no reachable sync source, retrying");
        tokio::time::sleep(self.timing.retry_delay).await;
        Flow::Continue
    }

    /// Peers that could relay the leader's feed, most recently connected first.
    async fn relay_candidates(&self, leader: I::Address) -> Vec<I::Address> {
        let peers = self.state.peers.lock().await;
        let mut candidates = peers
            .values()
            .filter(|peer| peer.addr != leader && peer.addr != self.state.my_address)
            .map(|peer| (peer.connect_status.is_connected(), peer.addr))
            .collect::<Vec<_>>();
        drop(peers);

        candidates.sort_by_key(|(connected, addr)| (std::cmp::Reverse(*connected), *addr));
        candidates.into_iter().map(|(_, addr)| addr).collect()
    }

    async fn sync_from(&mut self, target: I::Address, leader: I::Address) -> SyncAttempt {
        let connection = match self.io.connect(&target).await {
            Ok(connection) => connection,
            Err(error) => {
                tracing::debug!(?target, ?error, "failed to connect for state sync");
                return SyncAttempt::Unreachable;
            }
        };

        /* write must stay alive for the duration of the stream or the
         * connection closes */
        let (_remote, write, mut read) = connection.client_channels::<D>(self.settings.clone());

        let next_seq = match self.subscribe(&write, &mut read, target).await {
            Ok(next_seq) => next_seq,
            Err(error) => {
                tracing::warn!(?target, ?error, "state subscription failed");
                return SyncAttempt::Unreachable;
            }
        };

        self.stream(target, leader, next_seq, &mut read).await
    }

    /// Handshakes and subscribes, recovering the local state when the target
    /// can serve our position and resetting from a fresh snapshot otherwise.
    /// Returns the sequence the action feed will start at.
    async fn subscribe(
        &mut self,
        write: &Sender<SyncRequest<I::Address, D>>,
        read: &mut Receiver<SyncResponse<I::Address, D>>,
        target: I::Address,
    ) -> std::io::Result<u64> {
        let timeout = self.settings.message_timeout;

        tracing::info!(?target, "sync trace: handshake start");
        send(write, SyncRequest::ProtocolVersion(PROTOCOL_VERSION)).await?;
        expect_ok(recv(read, timeout).await?, "protocol version")?;

        send(write, SyncRequest::MyAddress(self.state.my_address)).await?;
        expect_ok(recv(read, timeout).await?, "my address")?;

        tracing::info!(?target, "sync trace: handshake done, settling recovery details");
        let details = self.state.state.settled_recovery_details().await;
        let local_next_seq = details.next_seq();
        tracing::info!(?target, local_next_seq, "sync trace: requesting recovery");
        send(write, SyncRequest::SubscribeRecovery(details)).await?;

        match recv(read, timeout).await? {
            SyncResponse::Accepted(next_seq) => {
                if next_seq != local_next_seq {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("recovery accepted at seq {next_seq} but local state expects {local_next_seq}"),
                    ));
                }
                tracing::info!(?target, next_seq, "recovered existing state through subscription");
                Ok(next_seq)
            }
            SyncResponse::RecoveryFailed => {
                tracing::info!(?target, "state recovery not possible, subscribing fresh");
                send(write, SyncRequest::SubscribeFresh).await?;

                match recv(read, timeout).await? {
                    SyncResponse::FreshState(fresh) => {
                        let next_seq = fresh.details().next_seq();
                        self.state.state.reset(fresh).await;
                        tracing::info!(?target, next_seq, "reset local state from fresh snapshot");
                        Ok(next_seq)
                    }
                    response => Err(unexpected("FreshState", &response)),
                }
            }
            response => Err(unexpected("Accepted or RecoveryFailed", &response)),
        }
    }

    /// Applies the authority action feed to the local state until the stream
    /// closes or the leader changes. Client actions received while streaming
    /// are forwarded to the sync target.
    async fn stream(
        &mut self,
        target: I::Address,
        leader: I::Address,
        mut expected_seq: u64,
        read: &mut Receiver<SyncResponse<I::Address, D>>,
    ) -> SyncAttempt {
        loop {
            tokio::select! {
                response = read.recv() => match response {
                    Some(SyncResponse::AuthorityAction(seq, action)) => {
                        if seq != expected_seq {
                            tracing::warn!(?target, seq, expected_seq, "action feed out of sequence, dropping subscription");
                            return SyncAttempt::Finished;
                        }
                        expected_seq += 1;
                        self.state.state.update(iter::once(action)).await;
                    }
                    Some(SyncResponse::ActionStreamClosed) | None => {
                        tracing::info!(?target, "state subscription closed");
                        return SyncAttempt::Finished;
                    }
                    Some(response) => {
                        tracing::warn!(?target, response = response.name(), "unexpected message on subscription stream");
                        return SyncAttempt::Finished;
                    }
                },
                action = self.actions_rx.recv() => {
                    let Some((source, action)) = action else {
                        return SyncAttempt::Shutdown;
                    };
                    let current = self.state.leader_state.lock().await.mode.clone();
                    if !matches!(&current, LeaderMode::Following { leader: still } if *still == leader) {
                        tracing::info!(?leader, "leader changed before forwarding action, dropping subscription");
                        self.drop_queued_actions();
                        return SyncAttempt::Finished;
                    }
                    self.forward_action(target, source, action).await;
                }
                _ = tokio::time::sleep(self.timing.leader_poll_interval) => {
                    let current = self.state.leader_state.lock().await.mode.clone();
                    if !matches!(&current, LeaderMode::Following { leader: still } if *still == leader) {
                        tracing::info!(?leader, "leader changed, dropping subscription");
                        self.drop_queued_actions();
                        return SyncAttempt::Finished;
                    }
                }
            }
        }
    }

    fn drop_queued_actions(&mut self) {
        while self.actions_rx.try_recv().is_ok() {}
    }

    async fn forward_action(&self, target: I::Address, source: I::Address, action: D::Action) {
        match self
            .peer_connections
            .send_rpc(target, SyncRequest::Action { source, action })
            .await
        {
            Ok(SyncResponse::Ok) => {}
            Ok(response) => {
                tracing::warn!(?target, ?source, response = response.name(), "sync target rejected forwarded action");
            }
            Err(error) => {
                tracing::warn!(?target, ?source, ?error, "failed to forward action to sync target");
            }
        }
    }
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
        response => Err(Error::new(
            ErrorKind::InvalidData,
            format!("expected Ok during {step}, got {}", response.name()),
        )),
    }
}

fn unexpected<A: SyncIOAddress, D: DeterministicState>(expected: &str, response: &SyncResponse<A, D>) -> Error {
    Error::new(ErrorKind::InvalidData, format!("expected {expected}, got {}", response.name()))
}
