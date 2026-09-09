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
    sync::Arc,
    time::Duration,
};

use message_encoding::MessageEncoding;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    cluster::{node_state::NodeState, peer_connections::PeerConnections},
    protocol::messages::{LeaderMode, LeadershipEpoch, PROTOCOL_VERSION, SyncRequest, SyncResponse},
    state::{
        deterministic_state::DeterministicState, recoverable_state::RecoverableStateAction,
        subscribable_state::StateHandle,
    },
    transport::{
        channels::NetIoSettings,
        traits::{SyncIO, SyncIOAddress},
    },
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
    Finished {
        applied_actions: bool,
    },
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
        }
    }

    pub async fn run(mut self) {
        loop {
            let leader_state = self.state.current_leader();
            let flow = match leader_state.mode {
                LeaderMode::Leading => self.lead(leader_state.epoch).await,
                LeaderMode::Following { leader } => self.follow(leader, leader_state.epoch).await,
                LeaderMode::NoLeader | LeaderMode::Electing => self.wait_for_leader().await,
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
        tokio::select! {
            _ = self.state.leadership_changed.notified() => {},
            _ = tokio::time::sleep(self.timing.leader_poll_interval) => {},
        }
        Flow::Continue
    }

    async fn lead(&mut self, epoch: LeadershipEpoch) -> Flow {
        loop {
            tokio::select! {
                action = self.actions_rx.recv() => {
                    let Some((source, action)) = action else {
                        return Flow::Shutdown;
                    };

                    if !self.state.valid_authority(epoch) {
                        return Flow::Continue;
                    }

                    let authority = self
                        .handle
                        .read_with(move |state| state.authority(RecoverableStateAction::StateAction { action }));
                    if !self.state.update_authoritative(epoch, authority).await {
                        return Flow::Continue;
                    }
                    tracing::debug!(?source, "applied action with local authority");
                }
                _ = self.state.leadership_changed.notified() => return Flow::Continue,
                _ = tokio::time::sleep(self.timing.leader_poll_interval) => {
                    if !self.state.valid_authority(epoch) {
                        tracing::info!(%epoch, "no longer leading, releasing authority");
                        return Flow::Continue;
                    }
                }
            }
        }
    }

    async fn follow(&mut self, leader: I::Address, epoch: LeadershipEpoch) -> Flow {
        /* the election logic should never point a follower at itself, but if
         * it ever does, subscribing to our own feed would idle forever (and
         * bounce forwarded actions back into our own queue) */
        if leader == self.state.my_address {
            tracing::warn!("leader state points at our own address, waiting for the election to settle");
            return self.wait_for_leader().await;
        }

        match self.sync_from(leader, leader, epoch).await {
            SyncAttempt::Finished { applied_actions } => return self.pace_resubscribe(applied_actions).await,
            SyncAttempt::LeaderChanged => return Flow::Continue,
            SyncAttempt::Shutdown => return Flow::Shutdown,
            SyncAttempt::Unreachable => {}
        }

        tracing::warn!(?leader, "cannot subscribe to leader directly, looking for a relay peer");

        for relay in self.relay_candidates(leader).await {
            match self.peer_connections.query_leader(relay).await {
                Ok(state)
                    if state.epoch == epoch
                        && matches!(&state.mode, LeaderMode::Following { leader: relayed } if *relayed == leader) => {}
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
            match self.sync_from(relay, leader, epoch).await {
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

    async fn sync_from(&mut self, target: I::Address, leader: I::Address, epoch: LeadershipEpoch) -> SyncAttempt {
        if !self.state.valid_replication(leader, epoch) {
            return SyncAttempt::LeaderChanged;
        }
        /* connect gives no timing guarantee, and an unbounded wait here would
         * also stop the task from noticing leader changes */
        let deadline = self
            .state
            .election_status()
            .valid_until
            .unwrap_or_else(tokio::time::Instant::now);
        let connection = match tokio::select! {
            connection = tokio::time::timeout(self.settings.message_timeout, self.io.connect(&target)) => connection,
            _ = self.state.leadership_changed.notified() => return SyncAttempt::LeaderChanged,
            _ = tokio::time::sleep_until(deadline) => return SyncAttempt::LeaderChanged,
        } {
            Ok(Ok(connection)) => connection,
            Ok(Err(error)) => {
                tracing::debug!(?target, ?error, "failed to connect for state sync");
                return SyncAttempt::Unreachable;
            }
            Err(_) => {
                tracing::debug!(?target, "timed out connecting for state sync");
                return SyncAttempt::Unreachable;
            }
        };

        /* write must stay alive for the duration of the stream or the
         * connection closes */
        let (_remote, write, mut read) = connection.client_channels::<D>(self.settings.clone());

        let next_seq = match self.subscribe(&write, &mut read, target, leader, epoch).await {
            Ok(next_seq) => next_seq,
            Err(error) => {
                tracing::warn!(?target, ?error, "state subscription failed");
                return SyncAttempt::Unreachable;
            }
        };

        self.stream(target, leader, epoch, next_seq, &mut read).await
    }

    /// Handshakes and subscribes, recovering the local state when the target
    /// can serve our position and resetting from a fresh snapshot otherwise.
    /// Returns the sequence the action feed will start at.
    async fn subscribe(
        &mut self,
        write: &Sender<SyncRequest<I::Address, D>>,
        read: &mut Receiver<SyncResponse<I::Address, D>>,
        target: I::Address,
        leader: I::Address,
        epoch: LeadershipEpoch,
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
            SyncResponse::Accepted(received_epoch, next_seq) => {
                if received_epoch != epoch || !self.state.valid_replication(leader, epoch) {
                    return Err(Error::new(ErrorKind::PermissionDenied, "subscription epoch is no longer valid"));
                }
                if next_seq != local_next_seq {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("recovery accepted at seq {next_seq} but local state expects {local_next_seq}"),
                    ));
                }
                tracing::info!(?target, next_seq, "recovered existing state through subscription");
                self.state.mark_synchronized(epoch);
                Ok(next_seq)
            }
            SyncResponse::RecoveryFailed => {
                tracing::info!(?target, "state recovery not possible, subscribing fresh");
                send(write, SyncRequest::SubscribeFresh).await?;

                match recv(read, timeout).await? {
                    SyncResponse::FreshState(received_epoch, fresh) => {
                        if received_epoch != epoch {
                            return Err(Error::new(ErrorKind::PermissionDenied, "snapshot epoch mismatch"));
                        }
                        let next_seq = fresh.details().next_seq();
                        if !self.state.reset_replica(leader, epoch, fresh).await {
                            return Err(Error::new(ErrorKind::PermissionDenied, "snapshot epoch expired"));
                        }
                        self.state.mark_synchronized(epoch);
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
        epoch: LeadershipEpoch,
        mut expected_seq: u64,
        read: &mut Receiver<SyncResponse<I::Address, D>>,
    ) -> SyncAttempt {
        let mut applied_actions = false;

        loop {
            tokio::select! {
                response = read.recv() => match response {
                    Some(SyncResponse::AuthorityAction(received_epoch, seq, action)) => {
                        if received_epoch != epoch || !self.state.valid_replication(leader, epoch) {
                            return SyncAttempt::LeaderChanged;
                        }
                        if seq != expected_seq {
                            tracing::warn!(?target, seq, expected_seq, "action feed out of sequence, dropping subscription");
                            return SyncAttempt::Finished { applied_actions };
                        }
                        expected_seq += 1;
                        if !self.state.update_replica(leader, epoch, action).await {
                            return SyncAttempt::LeaderChanged;
                        }
                        applied_actions = true;
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
                    if !self.state.valid_replication(leader, epoch) {
                        tracing::info!(?leader, "leader changed before forwarding action, dropping subscription");
                        self.drop_queued_actions();
                        return SyncAttempt::LeaderChanged;
                    }
                    self.forward_action(target, source, action).await;
                }
                _ = self.state.leadership_changed.notified() => {
                    if !self.state.valid_replication(leader, epoch) {
                        return SyncAttempt::LeaderChanged;
                    }
                }
                _ = tokio::time::sleep(self.timing.leader_poll_interval) => {
                    if !self.state.valid_replication(leader, epoch) {
                        tracing::info!(?leader, "leader changed, dropping subscription");
                        self.drop_queued_actions();
                        return SyncAttempt::LeaderChanged;
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
            .enqueue_forwarded_action(target, source, action)
            .await
        {
            Ok(()) => {}
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
        response => {
            Err(Error::new(ErrorKind::InvalidData, format!("expected Ok during {step}, got {}", response.name())))
        }
    }
}

fn unexpected<A: SyncIOAddress, D: DeterministicState>(expected: &str, response: &SyncResponse<A, D>) -> Error {
    Error::new(ErrorKind::InvalidData, format!("expected {expected}, got {}", response.name()))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Mutex as StdMutex,
            atomic::{AtomicU32, Ordering},
        },
    };

    use sequenced_broadcast::SequencedBroadcastSettings;
    use tokio::{
        io::{DuplexStream, ReadHalf, WriteHalf, duplex, split},
        sync::{Mutex, Notify, mpsc},
    };

    use super::*;
    use crate::{
        cluster::{election::LeadershipPermit, node_state::ElectionStatus},
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
        }
    }

    fn node_state(addr: u64, mode: LeaderMode<u64>) -> Arc<NodeState<u64, TestState>> {
        Arc::new(NodeState {
            my_address: addr,
            can_lead: true,
            peers: Mutex::new(HashMap::new()),
            state: SubscribableState::new(
                RecoverableState::new(addr, TestState(0)),
                SequencedBroadcastSettings::default(),
            )
            .unwrap(),
            election: std::sync::RwLock::new(status(addr, mode, LeadershipEpoch::default())),
            leadership_changed: Notify::new(),
            eligible: std::sync::atomic::AtomicBool::new(true),
            synced_epoch: std::sync::RwLock::new(None),
        })
    }

    fn status(addr: u64, mode: LeaderMode<u64>, epoch: LeadershipEpoch) -> ElectionStatus<u64> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        ElectionStatus {
            owner: Some(match mode {
                LeaderMode::Following { leader } => leader,
                _ => addr,
            }),
            epoch,
            ready: true,
            valid_until: Some(deadline),
            permit: matches!(mode, LeaderMode::Leading).then(|| LeadershipPermit::new(1, epoch, deadline)),
            error: None,
        }
    }

    fn start_task<I: SyncIO<Address = u64>>(
        state: &Arc<NodeState<u64, TestState>>,
        io: &Arc<I>,
    ) -> mpsc::Sender<(u64, u64)> {
        let connections = Arc::new(PeerConnections::new(io.clone(), test_settings(), state.clone()));
        let (actions_tx, actions_rx) = mpsc::channel(16);
        tokio::spawn(
            StateSyncTask::new(state.clone(), connections, io.clone(), test_settings(), actions_rx, test_timing())
                .run(),
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
        state.set_election_status(status(
            1,
            LeaderMode::Leading,
            LeadershipEpoch {
                incarnation: 1,
                revision: 1,
            },
        ));

        let mut handle = state.state.create_handle();
        /* and it must apply queued actions with local authority */
        actions_tx.send((1, 42)).await.unwrap();
        wait_until("an action applied with local authority", || handle.read_with(|state| state.state().0) == 1).await;
    }

    #[tokio::test(start_paused = true)]
    async fn following_own_address_waits_without_self_subscribing() {
        let state = node_state(1, LeaderMode::Following { leader: 1 });
        let io = Arc::new(HangingIo::default());
        let actions_tx = start_task(&state, &io);

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(io.connects.load(Ordering::SeqCst), 0, "the sync task must not subscribe to its own address");

        /* the task must still be responsive to leader changes */
        state.set_election_status(status(
            1,
            LeaderMode::Leading,
            LeadershipEpoch {
                incarnation: 1,
                revision: 1,
            },
        ));

        let mut handle = state.state.create_handle();
        actions_tx.send((1, 42)).await.unwrap();
        wait_until("an action after taking leadership", || handle.read_with(|state| state.state().0) == 1).await;
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
                    let _ = write
                        .send(SyncResponse::Accepted(LeadershipEpoch::default(), details.next_seq()))
                        .await;
                    return;
                }
                _ => SyncResponse::UnexpectedRequest,
            };
            if write.send(response).await.is_err() {
                return;
            }
        }
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
    #[tokio::test(start_paused = true)]
    async fn same_address_epoch_change_rejects_old_stream() {
        let state = node_state(1, LeaderMode::Following { leader: 2 });
        let old_epoch = LeadershipEpoch::default();
        let io = Arc::new(HangingIo::default());
        let connections = Arc::new(PeerConnections::new(io.clone(), test_settings(), state.clone()));
        let (_actions_tx, actions_rx) = mpsc::channel(16);
        let mut task = StateSyncTask::new(state.clone(), connections, io, test_settings(), actions_rx, test_timing());
        let (write, mut read) = mpsc::channel(16);
        write
            .send(SyncResponse::AuthorityAction(old_epoch, 0, RecoverableStateAction::StateAction { action: 1 }))
            .await
            .unwrap();
        state.set_election_status(status(
            1,
            LeaderMode::Following { leader: 2 },
            LeadershipEpoch {
                incarnation: 0,
                revision: 1,
            },
        ));
        assert!(matches!(task.stream(2, 2, old_epoch, 0, &mut read).await, SyncAttempt::LeaderChanged));
        assert_eq!(state.state.create_handle().read_with(|state| state.state().0), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_observation_rejects_replication_without_owner_change() {
        let state = node_state(1, LeaderMode::Following { leader: 2 });
        let epoch = LeadershipEpoch::default();
        tokio::time::advance(Duration::from_secs(61)).await;
        assert!(
            !state
                .update_replica(2, epoch, RecoverableStateAction::StateAction { action: 1 })
                .await
        );
        assert!(
            !state
                .reset_replica(2, epoch, RecoverableState::new(2, TestState(100)))
                .await
        );
        assert_eq!(state.state.create_handle().read_with(|state| state.state().0), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_leader_does_not_resume_authority() {
        let state = node_state(1, LeaderMode::Leading);
        let epoch = LeadershipEpoch::default();
        assert!(
            state
                .update_authoritative(epoch, RecoverableStateAction::StateAction { action: 1 })
                .await
        );
        tokio::time::advance(Duration::from_secs(61)).await;
        assert!(
            !state
                .update_authoritative(epoch, RecoverableStateAction::StateAction { action: 2 })
                .await
        );
        assert_eq!(state.state.create_handle().read_with(|state| state.state().0), 1);
    }
    #[tokio::test(start_paused = true)]
    async fn relay_cannot_serve_an_epoch_before_synchronizing() {
        use crate::cluster::rpc_server::{ResponseOrFeed, RpcServer};
        let state = node_state(1, LeaderMode::Following { leader: 2 });
        let (actions_tx, _actions_rx) = mpsc::channel(16);
        let server = RpcServer::new(state.clone(), actions_tx);
        assert!(matches!(
            server.handle(3, SyncRequest::SubscribeFresh).await,
            ResponseOrFeed::Response(SyncResponse::RecoveryFailed)
        ));
        let epoch = LeadershipEpoch::default();
        state.mark_synchronized(epoch);
        assert!(matches!(server.handle(3, SyncRequest::SubscribeFresh).await,
            ResponseOrFeed::FreshState { epoch: served, .. } if served == epoch));
        let next = LeadershipEpoch {
            incarnation: 0,
            revision: 1,
        };
        state.set_election_status(status(1, LeaderMode::Following { leader: 2 }, next));
        assert!(matches!(
            server.handle(3, SyncRequest::SubscribeFresh).await,
            ResponseOrFeed::Response(SyncResponse::RecoveryFailed)
        ));
        let mut preparing = status(1, LeaderMode::Leading, next);
        preparing.ready = false;
        state.set_election_status(preparing);
        assert!(matches!(
            server.handle(3, SyncRequest::SubscribeFresh).await,
            ResponseOrFeed::Response(SyncResponse::RecoveryFailed)
        ));
    }
}
