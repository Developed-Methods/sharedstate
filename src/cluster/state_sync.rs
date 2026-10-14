//! Drives the local deterministic state based on the node's leader state.
//!
//! When leading, client actions are made authoritative, applied to the state,
//! and pushed into the sequenced broadcast for subscribers. When following,
//! the task subscribes to the leader's action feed (recovering the existing
//! state when possible, resetting from a fresh snapshot otherwise) and
//! forwards client actions to the leader. If the leader cannot be reached
//! directly, the task relays through another peer after confirming over RPC
//! that the peer follows the same leader; the peer only accepts if it is fed
//! directly by the leader, so relay chains never grow past one hop or loop.
//!
//! An observer behind a voter gateway follows the gateway address itself:
//! whichever voter answers is the leader or fed directly by it, serves the
//! feed, and forwards actions on. Such an observer is a live source too, so
//! observers that cannot reach the gateway may relay through it.
//!
//! The task publishes its sync status on the node state so the rpc server
//! knows when this node is a live source others may subscribe to.
//!
//! A subscription can stay open on keepalives while delivering nothing, for
//! example when the target's own feed froze. When the feed has been quiet
//! for a while the task asks the target over RPC how far its state has
//! advanced, and drops the subscription if the target does not answer or
//! the feed fails to catch up to what it reported. A node fed directly by
//! the leader drops out of `SyncStatus::Direct` this way, which closes the
//! feeds of everyone relaying through it.

use std::{
    future::Future,
    io::{Error, ErrorKind},
    iter,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use message_encoding::MessageEncoding;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    cluster::{
        node_state::{ConnectStatus, NodeState, SyncStatus},
        peer_connections::{PeerConnections, PeerRpcError},
    },
    protocol::messages::{ElectionTerm, LeaderMode, SyncRequest, SyncResponse, PROTOCOL_VERSION},
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
    /// How long the action feed may go without delivering an action before
    /// the task checks over RPC that the sync target is still advancing. A
    /// subscription is dropped after one check where the target does not
    /// answer, or two quiet periods in a row where the target reports a
    /// state ahead of ours.
    pub feed_idle_timeout: Duration,
    /// How long a full state snapshot may take to arrive once requested.
    /// The transfer itself fails as soon as the connection stops making
    /// progress, so this only needs to cover a large snapshot over a slow
    /// link.
    pub snapshot_timeout: Duration,
}

impl Default for StateSyncTiming {
    fn default() -> Self {
        Self {
            leader_poll_interval: Duration::from_millis(100),
            retry_delay: Duration::from_millis(500),
            feed_idle_timeout: Duration::from_secs(10),
            snapshot_timeout: Duration::from_secs(120),
        }
    }
}

type SeqProbe = Pin<Box<dyn Future<Output = Result<u64, PeerRpcError>> + Send>>;

pub struct StateSyncTask<I: SyncIO, D: DeterministicState> {
    state: Arc<NodeState<I::Address, D>>,
    peer_connections: Arc<PeerConnections<I, D>>,
    io: Arc<I>,
    settings: NetIoSettings,
    actions_rx: Receiver<(I::Address, D::Action)>,
    handle: StateHandle<D>,
    timing: StateSyncTiming,
    last_drop_warning: Option<tokio::time::Instant>,
    /// A client action taken off `actions_rx` after the leader changed. It is
    /// handled before any newer action once the next subscription opens or
    /// this node starts leading.
    held_action: Option<(I::Address, D::Action)>,
}

/// Dropped forwarded actions are counted every time but logged at most this
/// often. A target that stopped answering drops every action the node
/// produces, and a warning per action would crowd out every other warning.
const DROP_WARNING_INTERVAL: Duration = Duration::from_secs(1);

enum Flow {
    Continue,
    Shutdown,
}

enum SyncAttempt {
    /// Could not establish a subscription with the target.
    Unreachable,
    /// A subscription ran and ended; the leader state should be re-checked.
    Finished { applied_actions: bool },
    /// The leader changed while streaming; re-check immediately.
    LeaderChanged,
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
            last_drop_warning: None,
            held_action: None,
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
        let flow = self.lead_until_displaced(term).await;
        self.set_sync_status(SyncStatus::NotSynced);
        flow
    }

    async fn lead_until_displaced(&mut self, term: ElectionTerm) -> Flow {
        /* bump the recovery generation so followers of the previous leader
         * can recover from us without a full state transfer */
        let new_id = unique_state_id(&self.state.my_address);
        self.state
            .state
            .update(iter::once(RecoverableStateAction::BumpGeneration { new_id }))
            .await;
        tracing::info!(%term, "leading, taking authority over shared state");
        self.set_sync_status(SyncStatus::Leading);

        if let Some((source, action)) = self.held_action.take() {
            self.apply_with_authority(source, action).await;
        }

        loop {
            tokio::select! {
                action = self.actions_rx.recv() => {
                    let Some((source, action)) = action else {
                        return Flow::Shutdown;
                    };

                    if !matches!(self.state.leader_state.lock().await.mode, LeaderMode::Leading) {
                        tracing::info!("no longer leading, holding queued action for the next sync source");
                        self.held_action = Some((source, action));
                        return Flow::Continue;
                    }

                    self.apply_with_authority(source, action).await;
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
        /* the election logic should never point a follower at itself, but if
         * it ever does, subscribing to our own feed would idle forever (and
         * bounce forwarded actions back into our own queue) */
        if leader == self.state.my_address {
            tracing::warn!("leader state points at our own address, waiting for the election to settle");
            return self.wait_for_leader().await;
        }

        match self.sync_from(leader, leader).await {
            SyncAttempt::Finished { applied_actions } => return self.pace_resubscribe(applied_actions).await,
            SyncAttempt::LeaderChanged => return Flow::Continue,
            SyncAttempt::Shutdown => return Flow::Shutdown,
            SyncAttempt::Unreachable => {}
        }

        tracing::warn!(?leader, "cannot subscribe to leader directly, looking for a relay peer");

        for relay in self.relay_candidates(leader).await {
            /* each query can spend several bounded dials on a dead peer, so
             * keep watching the leader state instead of finishing the scan */
            let leader_changed = self.until_leader_changes(leader);
            let queried = tokio::select! {
                queried = self.peer_connections.query_leader(relay) => queried,
                _ = leader_changed => {
                    tracing::info!(?leader, "leader changed during relay scan");
                    return Flow::Continue;
                }
            };
            match queried {
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
                SyncAttempt::Finished { applied_actions } => return self.pace_resubscribe(applied_actions).await,
                SyncAttempt::LeaderChanged => return Flow::Continue,
                SyncAttempt::Shutdown => return Flow::Shutdown,
                SyncAttempt::Unreachable => continue,
            }
        }

        tracing::warn!(?leader, "no reachable sync source, retrying");
        tokio::time::sleep(self.timing.retry_delay).await;
        Flow::Continue
    }

    /// A subscription that ended without delivering a single action is likely
    /// failing repeatedly (e.g. a sync source that keeps resetting), so pace
    /// the reconnect to avoid a hot loop of handshakes and state transfers.
    async fn pace_resubscribe(&self, applied_actions: bool) -> Flow {
        if !applied_actions {
            tokio::time::sleep(self.timing.retry_delay).await;
        }
        Flow::Continue
    }

    /// Peers that could relay the leader's feed: connected peers first,
    /// then peers we have not dialed, and peers whose last dial failed
    /// last. Each dead candidate costs several bounded dials. Behind a
    /// gateway the voters cannot be dialed, so only other observers qualify.
    async fn relay_candidates(&self, leader: I::Address) -> Vec<I::Address> {
        let behind_gateway = self.state.voter_gateway.is_some();
        let peers = self.state.peers.lock().await;
        let mut candidates = peers
            .values()
            .filter(|peer| peer.addr != leader && peer.addr != self.state.my_address)
            .filter(|peer| !(behind_gateway && peer.can_lead == Some(true)))
            .map(|peer| {
                let rank = match peer.connect_status {
                    ConnectStatus::Connected { .. } => 0,
                    ConnectStatus::NotConnected => 1,
                    ConnectStatus::FailedToConnect { .. } => 2,
                };
                (rank, peer.addr)
            })
            .collect::<Vec<_>>();
        drop(peers);

        candidates.sort();
        candidates.into_iter().map(|(_, addr)| addr).collect()
    }

    /// Resolves once the node's leader state no longer says to follow
    /// `leader`. The future owns its state handle so it can be raced against
    /// work that borrows the task.
    fn until_leader_changes(&self, leader: I::Address) -> impl std::future::Future<Output = ()> + 'static {
        let state = self.state.clone();
        let poll_interval = self.timing.leader_poll_interval;
        async move {
            loop {
                tokio::time::sleep(poll_interval).await;
                let current = state.leader_state.lock().await.mode.clone();
                if !matches!(&current, LeaderMode::Following { leader: still } if *still == leader) {
                    return;
                }
            }
        }
    }

    async fn sync_from(&mut self, target: I::Address, leader: I::Address) -> SyncAttempt {
        /* the connect and handshake are each bounded by message_timeout, but
         * together they can still hide a leader change for a long time */
        let leader_changed = self.until_leader_changes(leader);
        /* the write half is unused after the handshake but must stay alive
         * for the duration of the stream or the connection closes */
        let (_write, mut read, next_seq) = tokio::select! {
            opened = self.open_subscription(target) => match opened {
                Ok(opened) => opened,
                Err(error) => {
                    tracing::warn!(?target, ?error, "state subscription failed");
                    return SyncAttempt::Unreachable;
                }
            },
            _ = leader_changed => {
                tracing::info!(?target, ?leader, "leader changed while opening state subscription");
                return SyncAttempt::LeaderChanged;
            }
        };

        self.set_sync_status(if self.state.is_gateway(target) {
            SyncStatus::Gateway { gateway: target }
        } else if target == leader {
            SyncStatus::Direct { leader }
        } else {
            SyncStatus::Relayed { relay: target, leader }
        });
        let attempt = self.stream(target, leader, next_seq, &mut read).await;
        self.set_sync_status(SyncStatus::NotSynced);
        attempt
    }

    fn set_sync_status(&self, status: SyncStatus<I::Address>) {
        self.state.sync_status.send_if_modified(|current| {
            if *current == status {
                return false;
            }
            tracing::info!(previous = ?*current, ?status, "sync status updated");
            *current = status;
            true
        });
    }

    /// Connects to the target and completes the subscription handshake.
    async fn open_subscription(
        &mut self,
        target: I::Address,
    ) -> std::io::Result<(Sender<SyncRequest<I::Address, D>>, Receiver<SyncResponse<I::Address, D>>, u64)> {
        /* connect gives no timing guarantee, bound it like every other step */
        let connection = tokio::time::timeout(self.settings.message_timeout, self.io.connect(&target))
            .await
            .map_err(|_| Error::new(ErrorKind::TimedOut, "timed out connecting for state sync"))??;

        let (_remote, write, mut read) = connection.client_channels::<D>(self.settings.clone());
        let next_seq = self.subscribe(&write, &mut read, target).await?;
        Ok((write, read, next_seq))
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

        send(write, self.state.handshake_address()).await?;
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

                match recv(read, self.timing.snapshot_timeout).await? {
                    SyncResponse::FreshState(fresh) => {
                        let next_seq = fresh.details().next_seq();
                        self.state.state.reset(fresh).await;
                        tracing::info!(?target, next_seq, "reset local state from fresh snapshot");
                        Ok(next_seq)
                    }
                    SyncResponse::NotSynced => Err(not_synced()),
                    response => Err(unexpected("FreshState", &response)),
                }
            }
            SyncResponse::NotSynced => Err(not_synced()),
            response => Err(unexpected("Accepted or RecoveryFailed", &response)),
        }
    }

    /// Applies the authority action feed to the local state until the stream
    /// closes, stalls, or the leader changes. Client actions received while
    /// streaming are forwarded to the sync target.
    async fn stream(
        &mut self,
        target: I::Address,
        leader: I::Address,
        mut expected_seq: u64,
        read: &mut Receiver<SyncResponse<I::Address, D>>,
    ) -> SyncAttempt {
        let idle_timeout = self.timing.feed_idle_timeout;
        let mut applied_actions = false;
        let mut idle_deadline = tokio::time::Instant::now() + idle_timeout;
        /* set when a probe showed the target ahead of us; if the feed is
         * still quiet at the next deadline, it is stalled */
        let mut behind_target: Option<u64> = None;
        let mut probe: Option<SeqProbe> = None;

        if let Some((source, action)) = self.held_action.take() {
            self.forward_action(target, source, action).await;
        }

        loop {
            tokio::select! {
                response = read.recv() => match response {
                    Some(SyncResponse::AuthorityAction(seq, action)) => {
                        if seq != expected_seq {
                            tracing::warn!(?target, seq, expected_seq, "action feed out of sequence, dropping subscription");
                            return SyncAttempt::Finished { applied_actions };
                        }
                        expected_seq += 1;
                        self.state.state.update(iter::once(action)).await;
                        applied_actions = true;
                        idle_deadline = tokio::time::Instant::now() + idle_timeout;
                        behind_target = None;
                    }
                    Some(SyncResponse::ActionStreamClosed) | None => {
                        tracing::info!(?target, "state subscription closed");
                        return SyncAttempt::Finished { applied_actions };
                    }
                    Some(response) => {
                        tracing::warn!(?target, response = response.name(), "unexpected message on subscription stream");
                        return SyncAttempt::Finished { applied_actions };
                    }
                },
                action = self.actions_rx.recv() => {
                    let Some((source, action)) = action else {
                        return SyncAttempt::Shutdown;
                    };
                    let current = self.state.leader_state.lock().await.mode.clone();
                    if !matches!(&current, LeaderMode::Following { leader: still } if *still == leader) {
                        tracing::info!(?leader, "leader changed before forwarding action, dropping subscription");
                        self.held_action = Some((source, action));
                        return SyncAttempt::LeaderChanged;
                    }
                    self.forward_action(target, source, action).await;
                }
                _ = tokio::time::sleep_until(idle_deadline), if probe.is_none() => {
                    if let Some(target_seq) = behind_target {
                        tracing::warn!(
                            ?target, expected_seq, target_seq,
                            "action feed stalled behind sync target, dropping subscription"
                        );
                        self.state.counters.stalled_feeds_dropped.fetch_add(1, Ordering::Relaxed);
                        return SyncAttempt::Finished { applied_actions };
                    }
                    probe = Some(self.probe_next_seq(target, leader));
                }
                probed = async { probe.as_mut().expect("probe branch polled without a probe").await }, if probe.is_some() => {
                    probe = None;
                    match probed {
                        Ok(target_seq) if expected_seq < target_seq => {
                            tracing::info!(?target, expected_seq, target_seq, "action feed quiet while sync target is ahead");
                            behind_target = Some(target_seq);
                        }
                        Ok(_) => {}
                        Err(error) => {
                            tracing::warn!(?target, ?error, "sync target did not answer while the feed was quiet, dropping subscription");
                            self.state.counters.stalled_feeds_dropped.fetch_add(1, Ordering::Relaxed);
                            return SyncAttempt::Finished { applied_actions };
                        }
                    }
                    idle_deadline = tokio::time::Instant::now() + idle_timeout;
                }
                _ = tokio::time::sleep(self.timing.leader_poll_interval) => {
                    let current = self.state.leader_state.lock().await.mode.clone();
                    if !matches!(&current, LeaderMode::Following { leader: still } if *still == leader) {
                        tracing::info!(?leader, "leader changed, dropping subscription");
                        return SyncAttempt::LeaderChanged;
                    }
                }
            }
        }
    }

    /// Asks the sync target for the sequence its state has reached. Through
    /// a relay, the leader's last reported sequence counts too: a relay
    /// whose own feed froze still reports itself as live and its frozen
    /// sequence as current.
    fn probe_next_seq(&self, target: I::Address, leader: I::Address) -> SeqProbe {
        let state = self.state.clone();
        let peer_connections = self.peer_connections.clone();
        Box::pin(async move {
            let target_seq = peer_connections
                .query_leader_info(target)
                .await?
                .recovery_details
                .next_seq();
            if target == leader {
                return Ok(target_seq);
            }
            let leader_seq = state
                .peers
                .lock()
                .await
                .get(&leader)
                .and_then(|peer| peer.leader_info.as_ref())
                .map(|info| info.recovery_details.next_seq());
            Ok(target_seq.max(leader_seq.unwrap_or(0)))
        })
    }

    async fn apply_with_authority(&mut self, source: I::Address, action: D::Action) {
        let authority = self
            .handle
            .read_with(move |state| state.authority(RecoverableStateAction::StateAction { action }));
        self.state.state.update(iter::once(authority)).await;
        tracing::debug!(?source, "applied action with local authority");
    }

    async fn forward_action(&mut self, target: I::Address, source: I::Address, action: D::Action) {
        let Err(error) = self
            .peer_connections
            .try_enqueue_forwarded_action(target, source, action)
            .await
        else {
            return;
        };

        let counter = &self.state.counters.forwarded_actions_dropped;
        let dropped = counter.fetch_add(1, Ordering::Relaxed) + 1;
        if self
            .last_drop_warning
            .is_none_or(|at| DROP_WARNING_INTERVAL <= at.elapsed())
        {
            tracing::warn!(?target, ?source, ?error, dropped, "failed to forward action to sync target");
            self.last_drop_warning = Some(tokio::time::Instant::now());
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

fn not_synced() -> Error {
    Error::new(ErrorKind::NotConnected, "target is not a live source for the state")
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicU32, Ordering},
            Mutex as StdMutex,
        },
    };

    use sequenced_broadcast::SequencedBroadcastSettings;
    use tokio::{
        io::{duplex, split, DuplexStream, ReadHalf, WriteHalf},
        sync::{mpsc, watch, Mutex, Notify},
    };

    use super::*;
    use crate::{
        cluster::node_state::PeerState,
        protocol::messages::LeaderState,
        state::{recoverable_state::RecoverableState, subscribable_state::SubscribableState},
        transport::traits::SyncConnection,
    };

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestState(u64);

    impl DeterministicState for TestState {
        type Action = u64;
        type AuthorityAction = u64;

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
        fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
            self.0.write_to(out)
        }

        fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
            Ok(Self(MessageEncoding::read_from(read)?))
        }
    }

    fn test_settings() -> NetIoSettings {
        NetIoSettings {
            process_timeout: Duration::from_millis(100),
            message_timeout: Duration::from_millis(250),
        }
    }

    fn test_timing() -> StateSyncTiming {
        StateSyncTiming {
            leader_poll_interval: Duration::from_millis(20),
            retry_delay: Duration::from_millis(50),
            feed_idle_timeout: Duration::from_millis(500),
            snapshot_timeout: Duration::from_secs(5),
        }
    }

    fn node_state(addr: u64, mode: LeaderMode<u64>) -> Arc<NodeState<u64, TestState>> {
        Arc::new(NodeState {
            my_address: addr,
            can_lead: true,
            accessible: true,
            voter_gateway: None,
            gateway_view: Mutex::new(None),
            peers: Mutex::new(HashMap::new()),
            state: SubscribableState::new(
                RecoverableState::new(addr, TestState(0)),
                SequencedBroadcastSettings::default(),
            )
            .unwrap(),
            leader_state: Mutex::new(LeaderState {
                term: ElectionTerm::from_term(0),
                mode,
            }),
            sync_status: watch::Sender::new(SyncStatus::NotSynced),
            counters: Default::default(),
        })
    }

    fn start_task<I: SyncIO<Address = u64>>(
        state: &Arc<NodeState<u64, TestState>>,
        io: &Arc<I>,
    ) -> mpsc::Sender<(u64, u64)> {
        start_task_with_settings(state, io, test_settings())
    }

    fn start_task_with_settings<I: SyncIO<Address = u64>>(
        state: &Arc<NodeState<u64, TestState>>,
        io: &Arc<I>,
        settings: NetIoSettings,
    ) -> mpsc::Sender<(u64, u64)> {
        let connections = Arc::new(PeerConnections::new(io.clone(), settings.clone(), state.clone()));
        let (actions_tx, actions_rx) = mpsc::channel(16);
        tokio::spawn(
            StateSyncTask::new(state.clone(), connections, io.clone(), settings, actions_rx, test_timing()).run(),
        );
        actions_tx
    }

    async fn wait_until(what: &str, mut check: impl FnMut() -> bool) {
        for _ in 0..500 {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {what}");
    }

    /// Counts connect attempts and never completes them, like a transport
    /// whose remote silently drops SYNs.
    #[derive(Default)]
    struct HangingIo {
        connects: AtomicU32,
    }

    impl SyncIO for HangingIo {
        type Address = u64;
        type Read = ReadHalf<DuplexStream>;
        type Write = WriteHalf<DuplexStream>;

        async fn connect(&self, _remote: &u64) -> std::io::Result<SyncConnection<Self>> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn hanging_connect_does_not_block_leader_takeover() {
        let state = node_state(1, LeaderMode::Following { leader: 2 });
        let io = Arc::new(HangingIo::default());
        let actions_tx = start_task(&state, &io);

        /* park the sync task inside the hanging connect */
        wait_until("a connect attempt", || 1 <= io.connects.load(Ordering::SeqCst)).await;

        /* we win an election; the task must escape the connect and lead */
        *state.leader_state.lock().await = LeaderState {
            term: ElectionTerm::from_term(1),
            mode: LeaderMode::Leading,
        };

        let mut handle = state.state.create_handle();
        let initial_seq = handle.read_with(|state| state.details().next_seq());
        wait_until("the generation bump after taking leadership", || {
            initial_seq < handle.read_with(|state| state.details().next_seq())
        })
        .await;

        /* and it must apply queued actions with local authority */
        actions_tx.send((1, 42)).await.unwrap();
        wait_until("an action applied with local authority", || {
            handle.read_with(|state| state.state().0) == 1
        })
        .await;
    }

    /// A follower that cannot reach its leader scans every known peer as a
    /// relay candidate. Each candidate query dials through PeerConnections,
    /// which retries several times with message_timeout-bounded connects, so
    /// a handful of dead peers keeps the task away from `run()` for minutes.
    /// Once the node is told to lead, it must take over within about one
    /// bounded connect rather than finishing the whole relay scan first.
    #[tokio::test(start_paused = true)]
    async fn relay_scan_does_not_block_leader_takeover() {
        let settings = NetIoSettings::default();
        let state = node_state(1, LeaderMode::Following { leader: 2 });
        for peer in 3..=7 {
            state.peers.lock().await.insert(peer, PeerState::empty(peer));
        }

        let io = Arc::new(HangingIo::default());
        let _actions_tx = start_task_with_settings(&state, &io, settings.clone());
        let mut sync_status = state.sync_status.subscribe();

        /* the direct attempt on the leader times out, then the relay scan
         * starts dialing the first candidate */
        tokio::time::timeout(settings.message_timeout * 2, async {
            while io.connects.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed out waiting for the relay scan to start dialing");

        let decided_at = tokio::time::Instant::now();
        *state.leader_state.lock().await = LeaderState {
            term: ElectionTerm::from_term(1),
            mode: LeaderMode::Leading,
        };

        sync_status
            .wait_for(|status| *status == SyncStatus::Leading)
            .await
            .expect("sync task dropped its status before leading");
        let took = decided_at.elapsed();

        /* budget: the dial in flight when we decided may run to its timeout,
         * plus one poll interval of slack */
        let budget = settings.message_timeout + test_timing().leader_poll_interval;
        assert!(
            took <= budget,
            "took {took:?} from the leading decision to actually leading (budget {budget:?}); \
             connects={}",
            io.connects.load(Ordering::SeqCst)
        );
    }

    /// Behind a gateway the observer must not spend its relay scan dialing
    /// voters it can never reach: only other observers are candidates.
    #[tokio::test(start_paused = true)]
    async fn relay_scan_behind_gateway_skips_voters() {
        let state = Arc::new(NodeState {
            my_address: 1,
            can_lead: false,
            accessible: true,
            voter_gateway: Some(100),
            gateway_view: Mutex::new(None),
            peers: Mutex::new(HashMap::new()),
            state: SubscribableState::new(
                RecoverableState::new(1, TestState(0)),
                SequencedBroadcastSettings::default(),
            )
            .unwrap(),
            leader_state: Mutex::new(LeaderState {
                term: ElectionTerm::from_term(0),
                mode: LeaderMode::Following { leader: 100 },
            }),
            sync_status: watch::Sender::new(SyncStatus::NotSynced),
            counters: Default::default(),
        });
        {
            let mut peers = state.peers.lock().await;
            for voter in 2..=4 {
                let mut peer = PeerState::empty(voter);
                peer.can_lead = Some(true);
                peers.insert(voter, peer);
            }
            let mut observer = PeerState::empty(7);
            observer.can_lead = Some(false);
            peers.insert(7, observer);
        }

        let io = Arc::new(HangingIo::default());
        let connections = Arc::new(PeerConnections::new(io.clone(), test_settings(), state.clone()));
        let (_actions_tx, actions_rx) = mpsc::channel(16);
        let task = StateSyncTask::new(
            state.clone(),
            connections,
            io.clone(),
            test_settings(),
            actions_rx,
            test_timing(),
        );

        assert_eq!(task.relay_candidates(100).await, vec![7]);
    }

    #[tokio::test(start_paused = true)]
    async fn following_own_address_waits_without_self_subscribing() {
        let state = node_state(1, LeaderMode::Following { leader: 1 });
        let io = Arc::new(HangingIo::default());
        let _actions_tx = start_task(&state, &io);

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            io.connects.load(Ordering::SeqCst),
            0,
            "the sync task must not subscribe to its own address"
        );

        /* the task must still be responsive to leader changes */
        *state.leader_state.lock().await = LeaderState {
            term: ElectionTerm::from_term(1),
            mode: LeaderMode::Leading,
        };

        let mut handle = state.state.create_handle();
        wait_until("the generation bump after taking leadership", || {
            0 < handle.read_with(|state| state.details().next_seq())
        })
        .await;
    }

    /// Completes the sync handshake, accepts the recovery subscription, then
    /// immediately closes the connection without streaming a single action.
    struct ClosingSubscriptionIo {
        settings: NetIoSettings,
        connects: StdMutex<Vec<tokio::time::Instant>>,
        connected: Notify,
    }

    impl ClosingSubscriptionIo {
        fn new(settings: NetIoSettings) -> Self {
            Self {
                settings,
                connects: StdMutex::new(Vec::new()),
                connected: Notify::new(),
            }
        }

        fn connect_times(&self) -> Vec<tokio::time::Instant> {
            self.connects.lock().unwrap().clone()
        }
    }

    impl SyncIO for ClosingSubscriptionIo {
        type Address = u64;
        type Read = ReadHalf<DuplexStream>;
        type Write = WriteHalf<DuplexStream>;

        async fn connect(&self, remote: &u64) -> std::io::Result<SyncConnection<Self>> {
            self.connects.lock().unwrap().push(tokio::time::Instant::now());
            self.connected.notify_waiters();

            let (client, server) = duplex(64 * 1024);
            let (client_read, client_write) = split(client);
            let (server_read, server_write) = split(server);

            let (_, write, read) = SyncConnection::<Self> {
                remote: *remote,
                read: server_read,
                write: server_write,
            }
            .server_channels::<TestState>(self.settings.clone());
            tokio::spawn(accept_subscription_then_close(write, read));

            Ok(SyncConnection {
                remote: *remote,
                read: client_read,
                write: client_write,
            })
        }
    }

    async fn accept_subscription_then_close(
        write: Sender<SyncResponse<u64, TestState>>,
        mut read: Receiver<SyncRequest<u64, TestState>>,
    ) {
        while let Some(request) = read.recv().await {
            let response = match request {
                SyncRequest::ProtocolVersion(_) | SyncRequest::MyAddress(_) => SyncResponse::Ok,
                SyncRequest::SubscribeRecovery(details) => {
                    /* accept, then drop the connection without streaming */
                    let _ = write.send(SyncResponse::Accepted(details.next_seq())).await;
                    return;
                }
                _ => SyncResponse::UnexpectedRequest,
            };
            if write.send(response).await.is_err() {
                return;
            }
        }
    }

    /// Settings where keepalives hold an idle connection open, unlike
    /// `test_settings` whose read timeout is shorter than the keepalive
    /// interval.
    fn keepalive_settings() -> NetIoSettings {
        NetIoSettings {
            process_timeout: Duration::from_millis(100),
            message_timeout: Duration::from_secs(2),
        }
    }

    /// Accepts the recovery subscription and then keeps the feed open on
    /// keepalives without ever streaming an action, like a relay whose own
    /// feed froze. RPC connections answer leader info queries with
    /// `source`'s state, or never answer at all. Forwarded client actions
    /// are recorded with the address they were sent to.
    struct QuietFeedIo {
        settings: NetIoSettings,
        source: Arc<NodeState<u64, TestState>>,
        answers_rpc: bool,
        subscriptions: Arc<AtomicU32>,
        forwarded: Arc<StdMutex<Vec<(u64, u64)>>>,
    }

    impl QuietFeedIo {
        fn new(source: Arc<NodeState<u64, TestState>>, answers_rpc: bool) -> Self {
            Self {
                settings: keepalive_settings(),
                source,
                answers_rpc,
                subscriptions: Arc::new(AtomicU32::new(0)),
                forwarded: Arc::new(StdMutex::new(Vec::new())),
            }
        }

        fn forwarded(&self) -> Vec<(u64, u64)> {
            self.forwarded.lock().unwrap().clone()
        }
    }

    impl SyncIO for QuietFeedIo {
        type Address = u64;
        type Read = ReadHalf<DuplexStream>;
        type Write = WriteHalf<DuplexStream>;

        async fn connect(&self, remote: &u64) -> std::io::Result<SyncConnection<Self>> {
            let (client, server) = duplex(64 * 1024);
            let (client_read, client_write) = split(client);
            let (server_read, server_write) = split(server);

            let (_, write, read) = SyncConnection::<Self> {
                remote: *remote,
                read: server_read,
                write: server_write,
            }
            .server_channels::<TestState>(self.settings.clone());
            tokio::spawn(serve_quiet_feed(
                *remote,
                write,
                read,
                self.source.clone(),
                self.answers_rpc,
                self.subscriptions.clone(),
                self.forwarded.clone(),
            ));

            Ok(SyncConnection {
                remote: *remote,
                read: client_read,
                write: client_write,
            })
        }
    }

    async fn serve_quiet_feed(
        remote: u64,
        write: Sender<SyncResponse<u64, TestState>>,
        mut read: Receiver<SyncRequest<u64, TestState>>,
        source: Arc<NodeState<u64, TestState>>,
        answers_rpc: bool,
        subscriptions: Arc<AtomicU32>,
        forwarded: Arc<StdMutex<Vec<(u64, u64)>>>,
    ) {
        while let Some(request) = read.recv().await {
            let response = match request {
                SyncRequest::ProtocolVersion(_) | SyncRequest::MyAddress(_) => SyncResponse::Ok,
                SyncRequest::SubscribeRecovery(details) => {
                    subscriptions.fetch_add(1, Ordering::SeqCst);
                    SyncResponse::Accepted(details.next_seq())
                }
                SyncRequest::LeaderInfoQuery if answers_rpc => SyncResponse::LeaderInformation(source.leader_info().await),
                SyncRequest::Action { action, .. } if answers_rpc => {
                    forwarded.lock().unwrap().push((remote, action));
                    SyncResponse::Ok
                }
                SyncRequest::LeaderInfoQuery | SyncRequest::Action { .. } => continue,
                _ => SyncResponse::UnexpectedRequest,
            };
            if write.send(response).await.is_err() {
                return;
            }
        }
    }

    /// Advances `node`'s state by `count` actions.
    async fn advance(node: &NodeState<u64, TestState>, count: u64) {
        node.state
            .update((0..count).map(|action| RecoverableStateAction::StateAction { action }))
            .await;
    }

    struct QuietFeedOutcome {
        subscriptions: u32,
        took: Duration,
        stalled_feeds_dropped: u64,
    }

    async fn quiet_feed_resubscribes(source_ahead_by: u64, answers_rpc: bool) -> QuietFeedOutcome {
        let state = node_state(1, LeaderMode::Following { leader: 2 });
        let source = node_state(2, LeaderMode::Leading);
        advance(&source, source_ahead_by).await;
        let io = Arc::new(QuietFeedIo::new(source, answers_rpc));
        let _actions_tx = start_task_with_settings(&state, &io, keepalive_settings());

        wait_until("the first subscription", || 1 <= io.subscriptions.load(Ordering::SeqCst)).await;
        let subscribed_at = tokio::time::Instant::now();
        let window = test_timing().feed_idle_timeout * 2 + keepalive_settings().message_timeout * 3;
        let resubscribed = tokio::time::timeout(window, async {
            while io.subscriptions.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let took = if resubscribed.is_ok() { subscribed_at.elapsed() } else { window };
        QuietFeedOutcome {
            subscriptions: io.subscriptions.load(Ordering::SeqCst),
            took,
            stalled_feeds_dropped: state.counters.stalled_feeds_dropped.load(Ordering::Relaxed),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn quiet_feed_is_dropped_when_target_is_ahead() {
        let outcome = quiet_feed_resubscribes(3, true).await;
        assert!(2 <= outcome.subscriptions, "a feed stuck behind its target must be dropped");
        assert!(1 <= outcome.stalled_feeds_dropped, "the dropped feed must be counted");

        /* one quiet period to probe, one more to confirm the stall, plus
         * the rpc round trip and the resubscribe pacing */
        let timing = test_timing();
        let budget = timing.feed_idle_timeout * 2 + keepalive_settings().message_timeout + timing.retry_delay;
        let took = outcome.took;
        assert!(took <= budget, "took {took:?} to drop a stalled feed (budget {budget:?})");
    }

    #[tokio::test(start_paused = true)]
    async fn quiet_feed_is_dropped_when_target_does_not_answer() {
        let outcome = quiet_feed_resubscribes(0, false).await;
        assert!(2 <= outcome.subscriptions, "a feed whose target stopped answering must be dropped");
        assert!(1 <= outcome.stalled_feeds_dropped, "the dropped feed must be counted");

        let timing = test_timing();
        let budget = timing.feed_idle_timeout + keepalive_settings().message_timeout + timing.retry_delay;
        let took = outcome.took;
        assert!(took <= budget, "took {took:?} to drop an unresponsive target (budget {budget:?})");
    }

    #[tokio::test(start_paused = true)]
    async fn quiet_feed_is_kept_while_target_is_also_quiet() {
        let outcome = quiet_feed_resubscribes(0, true).await;
        assert_eq!(outcome.subscriptions, 1, "a quiet feed from a target with nothing new must be kept");
        assert_eq!(outcome.stalled_feeds_dropped, 0);
    }

    /// Through a relay, a frozen relay reports its own frozen sequence. The
    /// leader's last reported sequence must count as well.
    #[tokio::test(start_paused = true)]
    async fn quiet_relay_feed_is_dropped_when_leader_is_ahead() {
        let state = node_state(1, LeaderMode::Following { leader: 2 });
        let leader = node_state(2, LeaderMode::Leading);
        advance(&leader, 3).await;
        let mut leader_peer = PeerState::empty(2);
        leader_peer.leader_info = Some(leader.leader_info().await);
        state.peers.lock().await.insert(2, leader_peer);

        let relay = node_state(3, LeaderMode::Following { leader: 2 });
        let io = Arc::new(QuietFeedIo::new(relay, true));
        let connections = Arc::new(PeerConnections::new(io.clone(), keepalive_settings(), state.clone()));
        let (_actions_tx, actions_rx) = mpsc::channel(16);
        let mut task = StateSyncTask::new(
            state.clone(),
            connections,
            io.clone(),
            keepalive_settings(),
            actions_rx,
            test_timing(),
        );

        let attempt = tokio::time::timeout(test_timing().feed_idle_timeout * 6, task.sync_from(3, 2))
            .await
            .expect("a relay feed stuck behind the leader must be dropped");
        assert!(matches!(attempt, SyncAttempt::Finished { applied_actions: false }));
    }

    /// Client actions that arrive while the follower switches leaders must
    /// reach the new leader instead of being dropped with the old
    /// subscription.
    #[tokio::test(start_paused = true)]
    async fn actions_queued_during_leader_change_reach_the_new_leader() {
        let state = node_state(1, LeaderMode::Following { leader: 2 });
        let io = Arc::new(QuietFeedIo::new(node_state(2, LeaderMode::Leading), true));
        let actions_tx = start_task_with_settings(&state, &io, keepalive_settings());
        let mut sync_status = state.sync_status.subscribe();
        sync_status
            .wait_for(|status| *status == SyncStatus::Direct { leader: 2 })
            .await
            .unwrap();

        *state.leader_state.lock().await = LeaderState {
            term: ElectionTerm::from_term(1),
            mode: LeaderMode::Following { leader: 3 },
        };
        actions_tx.send((1, 10)).await.unwrap();
        actions_tx.send((1, 11)).await.unwrap();

        wait_until("both actions forwarded", || 2 <= io.forwarded().len()).await;
        assert_eq!(io.forwarded(), vec![(3, 10), (3, 11)]);
    }

    #[tokio::test(start_paused = true)]
    async fn resubscribing_is_paced_when_subscription_delivers_nothing() {
        let state = node_state(1, LeaderMode::Following { leader: 2 });
        let io = Arc::new(ClosingSubscriptionIo::new(test_settings()));
        let _actions_tx = start_task(&state, &io);

        const ATTEMPTS: usize = 6;
        loop {
            let connected = io.connected.notified();
            if ATTEMPTS <= io.connect_times().len() {
                break;
            }
            connected.await;
        }

        /* each reconnect after a no-progress subscription must be delayed;
         * allow slack for the first attempt landing mid-interval */
        let times = io.connect_times();
        let elapsed = times[ATTEMPTS - 1] - times[0];
        let minimum = test_timing().retry_delay * (ATTEMPTS as u32 - 2);
        assert!(
            minimum <= elapsed,
            "{ATTEMPTS} subscription attempts within {elapsed:?} are not paced (expected at least {minimum:?})"
        );
    }
}
