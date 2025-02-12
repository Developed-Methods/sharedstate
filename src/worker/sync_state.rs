use std::{collections::VecDeque, fmt::Debug, sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc}, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

use message_encoding::MessageEncoding;
use sequenced_broadcast::{SequencedBroadcast, SequencedBroadcastMetrics, SequencedBroadcastSettings, SequencedReceiver, SequencedSender};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{handshake::{ConnectedToLeader, ConnectionEstablished, HandshakeClient, HandshakeServer, LeaderChannels, NewClient, RecoveringConnection, WantsRecovery, WantsState}, io::{SyncConnection, SyncIO}, recoverable_state::{RecoverableState, RecoverableStateAction}, state::{DeterministicState, SharedState}, utils::{LogHelper, PanicHelper, TimeoutPanicHelper}};

use super::{message_relay::{ClientActionSender, MessageRelay, RecoverableActionMessages, SequencedMessages}, state_updater::{StateAndReceiver, StateUpdater}};

pub struct SyncState<I: SyncIO, D: DeterministicState> {
    shared: SharedState<RecoverableState<D>>,
    event_tx: Sender<Event<I, D>>,
    client_actions_tx: Sender<D::Action>,
    cancel: CancellationToken,
    metrics: Arc<SyncStateMetrics>,
}

#[derive(Default, Debug)]
pub struct SyncStateMetrics {
    pub broadcast: SequencedBroadcastMetrics,
    pub broadcast_last_update: AtomicU64,

    pub connection_attempts: AtomicU64,
    pub last_seen: AtomicU64,

    pub connecting: AtomicBool,
    pub connected: AtomicBool,
    pub leading: AtomicBool,

    pub authority_relay_worker_loops: AtomicU64,
    pub action_relay_worker_loops: AtomicU64,
    pub state_updater_loops: AtomicU64,
}

enum Event<I: SyncIO, D: DeterministicState> {
    NewLeader(I::Address),
    AttemptConnect(u64, VecDeque<I::Address>),
    FreshConnection(ConnectedToLeader<I, D>, RecoverableState<D>),
    RecoveringConnection(RecoveringConnection<I>),
    NewClient(SyncConnection<I>),
    NewFreshClient(WantsState<I>),
    NewClientWantsRecovery(WantsRecovery<I>),
}

impl<I: SyncIO, D: DeterministicState> Debug for Event<I, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NewLeader(addr) => write!(f, "NewLeader({:?})", addr),
            Self::AttemptConnect(attempt, opts) => write!(f, "AttemptConnect(attempt: {}, options: {})", attempt, opts.len()),
            Self::FreshConnection(_, state) => write!(f, "FreshConnection(seq: {})", state.sequence()),
            Self::RecoveringConnection(recov) => write!(f, "RecoveringConnection(local_seq: {}, leader_seq: {})", recov.local_sequence(), recov.leader_sequence()),
            Self::NewClient(s) => write!(f, "NewClient({:?})", s.remote),
            Self::NewFreshClient(s) => write!(f, "NewFreshClient({:?})", s.remote()), 
            Self::NewClientWantsRecovery(s) => write!(f, "NewClientWantsRecovery(remote: {:?}, seq: {})", s.remote(), s.details().sequence), 
        }
    }
}

struct SyncStateWorker<I: SyncIO, D: DeterministicState> {
    io: Arc<I>,

    local: I::Address,
    leader: I::Address,
    peers: Vec<I::Address>,

    shared: SharedState<RecoverableState<D>>,
    event_tx: Sender<Event<I, D>>,
    event_rx: Receiver<Event<I, D>>,
    connect_attempts: Arc<AtomicU64>,

    updater: StateUpdater<RecoverableState<D>>,
    actions_tx: Sender<D::Action>,
    action_relay: MessageRelay<RecoverableActionMessages<D::Action>>,
    authority_relay: MessageRelay<SequencedMessages<RecoverableStateAction<D::AuthorityAction>>>,

    broadcast_settings: SequencedBroadcastSettings,
    broadcast: SequencedBroadcast<RecoverableStateAction<D::AuthorityAction>>,

    cancel: CancellationToken,
    metrics: Arc<SyncStateMetrics>,
}

impl<I: SyncIO, D: DeterministicState + MessageEncoding> SyncState<I, D> 
    where D::Action: MessageEncoding,
          D::AuthorityAction: MessageEncoding + Clone
 {
    pub fn new(
        io: Arc<I>,
        local: I::Address,
        leader: I::Address,
        state: RecoverableState<D>,
        broadcast_settings: SequencedBroadcastSettings,
    ) -> Self {
        let cancel = CancellationToken::new();
        let (event_tx, event_rx) = channel(1024);

        let worker_span = tracing::info_span!("SyncStateWorker", ?local);
        let _span = worker_span.clone().entered();

        let (broadcast, broadcast_tx) = SequencedBroadcast::new(state.sequence(), broadcast_settings.clone());
        let (shared, updater) = SharedState::new(state);

        let (actions_tx, actions_rx) = channel::<D::Action>(1024);

        let metrics = Arc::new(SyncStateMetrics::default());
        metrics.broadcast.update(broadcast.metrics_ref());

        let worker = if local == leader {
            let (lead_actions_tx, lead_actions_rx) = channel(1024);
            let action_relay = MessageRelay::new(actions_rx, ClientActionSender::ToLocalLeader(lead_actions_tx));

            let (updater, leader_rx) = StateUpdater::leader(lead_actions_rx, updater.into_lead());
            let authority_relay = MessageRelay::new(leader_rx, broadcast_tx);

            metrics.leading.store(true, Ordering::Release);

            SyncStateWorker {
                io,

                leader,
                local,
                peers: vec![],

                shared: shared.clone(),
                event_tx: event_tx.clone(),
                event_rx,
                connect_attempts: Arc::new(AtomicU64::new(0)),

                updater,
                actions_tx: actions_tx.clone(),
                action_relay,
                authority_relay,
                broadcast_settings,
                broadcast,

                cancel: cancel.clone(),
                metrics: metrics.clone(),
            }
        } else {
            event_tx
                .try_send(Event::AttemptConnect(0, VecDeque::new()))
                .expect("failed to queue event");

            let leader_rx = SequencedReceiver::new(updater.next_sequence(), channel(1).1);
            let (updater, authority_rx) = StateUpdater::follower(leader_rx, updater.into_follow());

            let action_relay = MessageRelay::new(actions_rx, ClientActionSender::ToRemote(channel(1).0));
            let authority_relay = MessageRelay::new(authority_rx, broadcast_tx);

            metrics.connecting.store(true, Ordering::Release);

            SyncStateWorker {
                io,

                leader,
                local,
                peers: vec![],

                shared: shared.clone(),
                event_tx: event_tx.clone(),
                event_rx,
                connect_attempts: Arc::new(AtomicU64::new(0)),

                updater,
                actions_tx: actions_tx.clone(),
                action_relay,
                authority_relay,
                broadcast_settings,
                broadcast,

                cancel: cancel.clone(),
                metrics: metrics.clone(),
            }
        };

        tokio::spawn(worker.start().instrument(worker_span));

        Self {
            shared,
            event_tx,
            client_actions_tx: actions_tx,
            cancel,
            metrics,
        }
    }

    pub fn shared(&self) -> SharedState<RecoverableState<D>> {
        self.shared.clone()
    }

    pub async fn set_leader(&self, leader: I::Address) {
        self.event_tx.send(Event::NewLeader(leader)).await.panic("worker closed");
    }

    pub fn actions_tx(&self) -> Sender<D::Action> {
        self.client_actions_tx.clone()
    }

    pub fn metrics_ref(&self) -> &SyncStateMetrics {
        &self.metrics
    }

    pub fn metrics(&self) -> Arc<SyncStateMetrics> {
        self.metrics.clone()
    }
}

impl<I: SyncIO, D: DeterministicState> Drop for SyncState<I, D> {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl<I: SyncIO, D: DeterministicState + MessageEncoding> SyncStateWorker<I, D>
where D::Action: MessageEncoding,
      D::AuthorityAction: MessageEncoding + Clone
{
    async fn start(mut self) {
        {
            let io = self.io.clone();
            let event_tx = self.event_tx.clone();
            let cancel = self.cancel.clone();

            tokio::spawn(async move {
                loop {
                    tokio::task::yield_now().await;

                    tokio::select! {
                        client_res = io.next_client() => {
                            let Ok(client) = client_res.err_log("failed to get next_client") else {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            };

                            event_tx.send(Event::NewClient(client)).await.panic("worker closed");
                        }
                        _ = cancel.cancelled() => {
                            break;
                        }
                    };
                }

                tracing::info!("stopping next_client acceptor");
            }.instrument(tracing::Span::current()));
        }

        loop {
            tokio::task::yield_now().await;

            let event = tokio::select! {
                event_opt = self.event_rx.recv() => {
                    event_opt.expect("should not be possible, have local reference")
                }
                _ = tokio::time::sleep(Duration::from_millis(128)) => {
                    self.update_metrics();
                    continue;
                }
                _ = self.cancel.cancelled() => {
                    break;
                }
            };

            tracing::info!("Process Event: {:?}", event);
            self.update_metrics();

            match event {
                Event::NewLeader(leader) => {
                    if self.leader == leader {
                        tracing::info!("no leader change");
                        continue;
                    }

                    let old_leader = std::mem::replace(&mut self.leader, leader);
                    if self.leader == self.local {
                        tracing::info!(?old_leader, "becoming leader");

                        self.metrics.leading.store(true, Ordering::Release);
                        self.metrics.connected.store(false, Ordering::Release);
                        self.metrics.connecting.store(false, Ordering::Release);
                        self.metrics.connection_attempts.store(0, Ordering::Release);

                        let (actions_tx, actions_rx) = channel(1024);
                        actions_tx.try_send(RecoverableStateAction::BumpGeneration).panic("failed to queue first action");

                        let _ = self.action_relay.replace_output(ClientActionSender::ToLocalLeader(actions_tx))
                            .timeout(Duration::from_millis(100), "replace action queue output").await
                            .panic("failed to replace action relay output");

                        let mut update = self.updater.update_internals(false)
                            .timeout(Duration::from_millis(100), "update internals to move to leader").await;

                        if update.become_leader(|_state| false, actions_rx).is_some() {
                            panic!("feed should not need update moving from local to lead");
                        }
                    } else {
                        let mut update = self.updater.update_internals(false)
                            .timeout(Duration::from_millis(100), "update internals").await;
                        let next_seq = update.next_sequence();

                        let needs_connect = if old_leader == self.local {
                            tracing::info!(new_leader = ?self.leader, next_seq, "becoming follower");
                            true
                        }
                        else {
                            tracing::info!(new_leader = ?self.leader, ?old_leader, next_seq, "leader changed");
                            false
                        };

                        /* setup follow with dead end as need to a new connection to leader */
                        update.setup_follow(SequencedReceiver::new(update.next_sequence(), channel(1).1), |_| ())
                            .panic("invalid sequence");

                        let mut leader_options = VecDeque::new();
                        leader_options.push_back(self.leader.clone());
                        leader_options.extend(self.peers.iter().cloned());

                        let event_tx = self.event_tx.clone();
                        let attempt = self.connect_attempts.clone();

                        self.metrics.leading.store(false, Ordering::Release);
                        self.metrics.connected.store(false, Ordering::Release);
                        self.metrics.connecting.store(true, Ordering::Release);
                        self.metrics.connection_attempts.store(0, Ordering::Release);

                        if needs_connect {
                            tokio::spawn(async move {
                                let at = attempt.fetch_add(1, Ordering::SeqCst) + 1;
                                event_tx.send(Event::AttemptConnect(at, leader_options)).await.panic("worker crashed");
                            }.instrument(tracing::Span::current()));
                        }
                    }
                }
                Event::AttemptConnect(attempt, mut options) => {
                    if self.leader == self.local {
                        tracing::info!("attempting connect but currently leader");
                        continue;
                    }

                    if attempt != self.connect_attempts.load(Ordering::SeqCst) {
                        tracing::info!("ignoring connection attempt and new item queued");
                        continue;
                    }

                    self.metrics.connection_attempts.fetch_add(1, Ordering::AcqRel);
                    self.metrics.leading.store(false, Ordering::Release);
                    self.metrics.connected.store(false, Ordering::Release);
                    self.metrics.connecting.store(true, Ordering::Release);

                    if options.is_empty() {
                        options.push_back(self.leader.clone());
                        options.extend(self.peers.iter().cloned());
                    }

                    let event_tx = self.event_tx.clone();
                    let attempts = self.connect_attempts.clone();
                    let addr = options.pop_front().unwrap();
                    let io = self.io.clone();

                    let state_details = self.shared.read().details().clone();

                    let span = tracing::info_span!("AttemptConnect", ?addr);

                    tokio::spawn(async move {
                        let Ok(conn) = io.connect(&addr).await.err_log("failed to connect to leader") else {
                            tokio::time::sleep(Duration::from_millis(500)).await;

                            let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                            event_tx.send(Event::AttemptConnect(attempt, options)).await.panic("worker crashed");

                            return;
                        };

                        let handshake = HandshakeClient::new(conn);

                        let established_res = if 0 < state_details.state_sequence {
                            tracing::info!("attempt reconnect: {:?}", state_details);
                            handshake.reconnect(state_details).await.err_log("failed to reconnect")
                        } else {
                            tracing::info!("attempt fresh connect");
                            handshake.connect().await.err_log("failed to connect")
                        };

                        let Ok(established) = established_res else {
                            tokio::time::sleep(Duration::from_millis(500)).await;

                            let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                            event_tx.send(Event::AttemptConnect(attempt, options)).await.panic("wroker crashed");

                            return;
                        };

                        match established {
                            ConnectionEstablished::FreshConnection(fresh) => {
                                let Ok((connection, state)) = fresh.load_state::<D>().await.err_log("failed to load state from fresh connection") else {
                                    tokio::time::sleep(Duration::from_millis(500)).await;

                                    let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                                    event_tx.send(Event::AttemptConnect(attempt, options)).await.panic("worker crashed");

                                    return;
                                };

                                event_tx.send(Event::FreshConnection(connection, state)).await.panic("worker crashed");
                            }
                            ConnectionEstablished::RecoveringConnection(recovering) => {
                                event_tx.send(Event::RecoveringConnection(recovering)).await.panic("worker crashed");
                            }
                        }
                    }.instrument(span));
                }
                Event::FreshConnection(connection, state) => {
                    if self.leader == self.local {
                        tracing::info!("drop new leader connection as local now leader");
                        continue;
                    }

                    let (_, LeaderChannels {
                        action_tx,
                        authority_rx,
                        reader_task
                    }) = connection.start_io_workers();

                    let state_and_rx = StateAndReceiver::create(state, authority_rx)
                        .panic("fresh connection has invalid sequence");

                    let authority_rx = self.updater
                        .update_internals(false)
                        .timeout(Duration::from_millis(100), "update internals").await
                        .reset_follow(state_and_rx);
                    
                    self.action_relay
                        .replace_output(ClientActionSender::ToRemote(action_tx))
                        .timeout(Duration::from_millis(100), "timeout replacing output").await
                        .panic("failed to replace output");

                    /* replace broadcast and drop all subscribers */
                    let (broadcast, broadcast_tx) = SequencedBroadcast::new(
                        authority_rx.next_seq(),
                        self.broadcast_settings.clone()
                    );
                    self.broadcast = broadcast;
                    self.authority_relay = MessageRelay::new(authority_rx, broadcast_tx);

                    self.metrics.leading.store(false, Ordering::Release);
                    self.metrics.connected.store(true, Ordering::Release);
                    self.metrics.connecting.store(false, Ordering::Release);
                    self.metrics.connection_attempts.store(0, Ordering::Release);

                    let event_tx = self.event_tx.clone();
                    let attempts = self.connect_attempts.clone();
                    let metrics = self.metrics.clone();

                    tokio::spawn(async move {
                        let _ = reader_task.await.err_log("failed to join reader task");
                        tracing::info!("Connection to leader closed, sending new connection attempt");
                        metrics.connection_attempts.store(0, Ordering::Release);

                        let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                        event_tx.send(Event::AttemptConnect(attempt, VecDeque::new())).await.panic("worker closed");
                    }.instrument(tracing::Span::current()));
                }
                Event::RecoveringConnection(recovering) => {
                    if self.leader == self.local {
                        tracing::info!("drop new leader connection as local now leader");
                        continue;
                    }

                    let (leader_tx, leader_rx) = channel(1024);
                    let leader_tx = SequencedSender::new(recovering.local_sequence(), leader_tx);
                    let leader_rx = SequencedReceiver::new(recovering.local_sequence(), leader_rx);

                    let connection = self.updater
                        .update_internals(false)
                        .timeout(Duration::from_millis(100), "update internals").await
                        .setup_follow(leader_rx, |state| recovering.recover(state))
                        .panic("recovering with invalid sequence")
                        .panic("state updated during connection, sequence change");

                    let (_, action_tx, reader_task) = connection.start_io_workers2(leader_tx).panic("invalid sequence");

                    self.action_relay
                        .replace_output(ClientActionSender::ToRemote(action_tx))
                        .timeout(Duration::from_millis(100), "timeout replacing output").await
                        .panic("failed to replace output");

                    self.metrics.leading.store(false, Ordering::Release);
                    self.metrics.connected.store(true, Ordering::Release);
                    self.metrics.connecting.store(false, Ordering::Release);
                    self.metrics.connection_attempts.store(0, Ordering::Release);

                    let event_tx = self.event_tx.clone();
                    let attempts = self.connect_attempts.clone();
                    let metrics = self.metrics.clone();

                    tokio::spawn(async move {
                        let _ = reader_task.await.err_log("failed to join reader task");
                        metrics.connection_attempts.store(0, Ordering::Release);

                        tracing::info!("Connection to leader closed, sending new connection attempt");
                        let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                        event_tx.send(Event::AttemptConnect(attempt, VecDeque::new())).await.panic("worker closed");
                    }.instrument(tracing::Span::current()));
                }
                Event::NewClient(conn) => {
                    let event_tx = self.event_tx.clone();

                    tokio::spawn(async move {
                        let Ok(accepted) = HandshakeServer::new(conn)
                            .accept().await
                            .err_log("failed to accept new client") else { return; };

                        match accepted {
                            NewClient::Fresh(fresh) => {
                                event_tx.send(Event::NewFreshClient(fresh)).await.panic("worker crashed");
                            }
                            NewClient::WithData(with_data) => {
                                event_tx.send(Event::NewClientWantsRecovery(with_data)).await.panic("worker crashed");
                            }
                        }
                    }.instrument(tracing::Span::current()));
                }
                Event::NewFreshClient(fresh) => {
                    let state = self.shared.read().clone();

                    /* TODO: remove wait */
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    let Ok(authority_rx) = self.broadcast.add_client(state.sequence(), true)
                        .timeout(Duration::from_millis(100), "failed to add client").await
                        .err_log("failed to add fresh client to broadcast") else { continue };

                    let actions_tx = self.actions_tx.clone();

                    tokio::spawn(async move {
                        let Ok(ready) = fresh.send_state(&state).await
                            .err_log("failed to send client fresh state") else { return };

                        let remote = ready.start_io_tasks2(actions_tx, authority_rx)
                            .log().panic("sequence and state sequences do not match");

                        tracing::info!(?remote, "Fresh client accepted");
                    }.instrument(tracing::Span::current()));
                }
                Event::NewClientWantsRecovery(recover) => {
                    let details = self.shared.read().details().clone();
                    let can_recover = details.can_recover_follower(recover.details());

                    let authority_rx_opt = if can_recover {
                        self.broadcast.add_client(recover.details().sequence, true)
                            .timeout(Duration::from_millis(100), "failed to add client").await
                            .ok()
                    } else {
                        None
                    };

                    let actions_tx = self.actions_tx.clone();

                    if let Some(authority_rx) = authority_rx_opt {
                        tokio::spawn(async move {
                            let leader_seq = details.sequence;
                            let client_seq = authority_rx.next_seq();

                            let Ok(ready) = recover
                                .accept_recover::<D>(details).await
                                .err_log("failed to recover state") else { return };

                            let remote = ready.start_io_tasks2(actions_tx, authority_rx)
                                .log().panic("invalid sequence");

                            tracing::info!(?remote, client_seq, leader_seq, "Recover client accepted");
                        }.instrument(tracing::Span::current()));
                    }
                    else {
                        let state = self.shared.read().clone();

                        let Ok(authority_rx) = self.broadcast.add_client(state.sequence(), true)
                            .timeout(Duration::from_millis(100), "failed to add client").await
                            .err_log("failed to add new fresh client") else { continue };

                        tokio::spawn(async move {
                            let Ok(ready) = recover
                                .accept_client_with_state(&state).await
                                .err_log("failed to send client new state") else { return };

                            let remote = ready.start_io_tasks2(actions_tx, authority_rx)
                                .log().panic("invalid sequence");

                            tracing::info!(?remote, "Recovery rejected client accepted with fresh state");
                        }.instrument(tracing::Span::current()));
                    }
                }
            }
        }

        let _ = async move {
            tracing::info!(events_tx_count = self.event_tx.strong_count(), "starting shutdown");
            
            /* drop event_tx to flush out event_rx */
            drop(self.event_tx);

            let mut count = 0;
            while self.event_rx.recv().await.is_some() {
                count += 1;
            }

            tracing::info!(dead_events = count, "worker shutdown complete");
        }.await.instrument(tracing::info_span!("SyncStateWorker::shutdown"));
    }

    fn update_metrics(&self) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        self.metrics.last_seen.store(now, Ordering::Relaxed);
        self.metrics.action_relay_worker_loops.store(self.action_relay.worker_loops(), Ordering::Relaxed);
        self.metrics.authority_relay_worker_loops.store(self.authority_relay.worker_loops(), Ordering::Relaxed);

        let last_broadcast = self.metrics.broadcast_last_update.load(Ordering::Acquire);
        if 100 < now.max(last_broadcast) - last_broadcast {
            self.metrics.broadcast.update(self.broadcast.metrics_ref());
            self.metrics.broadcast_last_update.store(now, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod test {
    use crate::testing::{setup_logging, state_tests::{TestState, TestStateAction}, test_sync_io::TestSyncNet};
    use super::*;

    #[tokio::test]
    async fn sync_state_drop_test() {
        setup_logging();

        let net = TestSyncNet::new();

        let a = net.io(1).await;
        let b = net.io(2).await;
        let c = net.io(3).await;

        let sync_a = SyncState::new(a, 1, 1, RecoverableState::new(100, TestState::default()), SequencedBroadcastSettings::default());
        let sync_b = SyncState::new(b, 2, 1, RecoverableState::new(200, TestState::default()), SequencedBroadcastSettings::default());
        let sync_c = SyncState::new(c, 3, 1, RecoverableState::new(100, TestState::default()), SequencedBroadcastSettings::default());

        tokio::time::sleep(Duration::from_millis(10)).await;

        drop(sync_a);
        drop(sync_b);
        drop(sync_c);

        tokio::time::sleep(Duration::from_secs(5)).await;

        tracing::info!("should be done");
    }

    #[tokio::test]
    async fn sync_state_test() {
        setup_logging();

        let net = TestSyncNet::new();

        let a = net.io(1).await;
        let b = net.io(2).await;
        let c = net.io(3).await;

        let sync_a = SyncState::new(a, 1, 1, RecoverableState::new(100, TestState::default()), SequencedBroadcastSettings::default());
        let sync_b = SyncState::new(b, 2, 1, RecoverableState::new(200, TestState::default()), SequencedBroadcastSettings::default());
        let sync_c = SyncState::new(c, 3, 1, RecoverableState::new(100, TestState::default()), SequencedBroadcastSettings::default());

        let shared_a = sync_a.shared.clone();
        let shared_b = sync_b.shared.clone();
        let shared_c = sync_c.shared.clone();

        let action_a = sync_a.client_actions_tx.clone();
        let action_b = sync_b.client_actions_tx.clone();
        let action_c = sync_c.client_actions_tx.clone();

        action_a.send(TestStateAction::Add { slot: 1, value: 1 }).await.unwrap();

        tokio::time::sleep(Duration::from_millis(1_000)).await;

        assert_eq!(shared_a.read().details().id, 100);
        assert_eq!(shared_b.read().details().id, 100);
        assert_eq!(shared_c.read().details().id, 100);

        action_b.send(TestStateAction::Add { slot: 1, value: 10 }).await.unwrap();
        action_c.send(TestStateAction::Add { slot: 1, value: 100 }).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(shared_a.read().state().numbers[1], 111);
        assert_eq!(shared_b.read().state().numbers[1], 111);
        assert_eq!(shared_c.read().state().numbers[1], 111);

        sync_a.set_leader(2).await;
        sync_b.set_leader(2).await;
        sync_c.set_leader(2).await;

        tokio::time::sleep(Duration::from_millis(4_000)).await;

        action_a.send(TestStateAction::Add { slot: 1, value: 1 }).await.unwrap();
        action_b.send(TestStateAction::Add { slot: 1, value: 10 }).await.unwrap();
        action_c.send(TestStateAction::Add { slot: 1, value: 100 }).await.unwrap();

        tracing::info!("new actions sent");
        tokio::time::sleep(Duration::from_millis(1_000)).await;

        assert_eq!(shared_a.read().state().numbers[1], 222);
        assert_eq!(shared_b.read().state().numbers[1], 222);
        assert_eq!(shared_c.read().state().numbers[1], 222);
    }
}

