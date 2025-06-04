use std::{cmp::Ordering, collections::{hash_map, HashMap}, sync::{atomic::AtomicU64, Arc}, task::Poll, time::Duration};

use arc_metrics::{IntCounter, IntGauge};
use futures_util::future::poll_fn;
use message_encoding::MessageEncoding;
use sequenced_broadcast::{SequencedBroadcastSettings, SequencedReceiver, SequencedSender};
use tokio::{sync::mpsc::{channel, Receiver, Sender}, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{net::{io::{SyncConnection, SyncIO}, message_channels::{NetIoSettings, ReadChannel, WriteChannel}, messages::{SyncRequest, SyncResponse}}, recoverable_state::{RecoverableStateAction, SourceId}, state::DeterministicState, utils::{now_ms, PanicHelper}, worker::sync_updater::FollowTarget};

use super::sync_updater::{NewFollowState, SyncUpdater, ValidNewFollowState};

pub struct SyncManagerWorker<I: SyncIO, D: DeterministicState> {
    io: I,

    local: I::Address,
    leader: I::Address,

    follow_state: Option<FollowState<I, D>>,

    updater: SyncUpdater<I::Address, D>,
    peers: HashMap<I::Address, DiscoveredPeer>,

    client_msg_tx: Sender<ClientMessage<I, D>>,
    client_msg_rx: Receiver<ClientMessage<I, D>>,
    net_settings: NetIoSettings,

    timers: Timers,
    
    metrics: Arc<SyncManagerMetrics>,
}

#[derive(Debug, Default)]
struct Timers {
    ensure_leader: TimerOpt,
    confirm_leader_timeout: TimerOpt,
    leader_failed: TimerOpt,
    send_ping: TimerOpt,
    pong_timeout: TimerOpt,
    connect_to_leader: TimerOpt,
}

struct FollowState<I: SyncIO, D: DeterministicState> {
    remote: I::Address,
    cancel: CancellationToken,
    to_leader: Sender<SyncRequest<I, D>>,
    from_leader: Receiver<SyncResponse<I, D>>,
    feed_updater: Option<SequencedSender<RecoverableStateAction<I::Address, D::AuthorityAction>>>,
}

#[derive(Debug, Default)]
pub struct SyncManagerMetrics {
    pub client_send_dropped: IntCounter,
    pub client_action_dropped: IntCounter,
    pub client_recovery_fails: IntCounter,
    pub client_fresh_fails: IntCounter,
    pub client_recovery_success: IntCounter,

    pub client_req_ping: IntCounter,
    pub client_req_send_me_peers: IntCounter,
    pub client_req_notice_peers: IntCounter,
    pub client_req_whois_leader: IntCounter,
    pub client_req_action: IntCounter,
    pub client_req_recover: IntCounter,
    pub client_req_fresh: IntCounter,

    pub event_queue_fail: IntCounter,
    pub client_dual_connect_error: IntCounter,

    pub clients_connected: IntGauge,
    pub clients_subscribed: IntGauge,
    pub error_failed_to_ask_leader: IntCounter,
    pub error_failed_to_ping_leader: IntCounter,
}

pub enum Event<I: SyncIO, D: DeterministicState> {
    ClientMessage(ClientMessage<I, D>),
    SetLeader(I::Address),
    ConnectToLeader,
    EnsureLeader,
    SendLeaderPing,
    LeaderMessage(SyncResponse<I, D>),
    CloseLeader(bool),
}

struct ClientMessage<I: SyncIO, D: DeterministicState> {
    client: Arc<ClientState<I::Address>>,
    msg: SyncRequest<I, D>,
    send: Sender<SyncResponse<I, D>>,
}

pub struct SyncMangerSettings {
    pub broadcast: SequencedBroadcastSettings,
    pub net_io: NetIoSettings,
}

impl<I: SyncIO, D: DeterministicState> SyncManagerWorker<I, D> where D: MessageEncoding, D::AuthorityAction: MessageEncoding + Clone, D::Action: MessageEncoding {
    pub fn new(io: I, local: I::Address, state: D, settings: SyncMangerSettings) -> Self {
        let (client_msg_tx, client_msg_rx) = channel(2048);

        SyncManagerWorker {
            io,
            local,
            leader: local,
            metrics: Arc::new(SyncManagerMetrics::default()),
            follow_state: None,
            client_msg_tx,
            client_msg_rx,
            peers: HashMap::new(),
            updater: SyncUpdater::new(local, state, settings.broadcast),
            timers: Timers::default(),
            net_settings: settings.net_io,
        }
    }

    pub async fn start(mut self) {
        loop {
            let read_from_leader = {
                let follow = self.follow_state.as_mut();

                async move {
                    let Some(follow) = follow else {
                        poll_fn(|_| Poll::<()>::Pending).await;
                        unreachable!()
                    };

                    follow.from_leader.recv().await
                }
            };

            let event = tokio::select! {
                msg_opt = self.client_msg_rx.recv() => {
                    Event::ClientMessage(msg_opt.panic("missing event"))
                }
                leader_opt = read_from_leader => {
                    match leader_opt {
                        None => Event::CloseLeader(true),
                        Some(msg) => Event::LeaderMessage(msg)
                    }
                }
                _ = self.timers.confirm_leader_timeout.wait_till() => {
                    Event::CloseLeader(true)
                }
                _ = self.timers.leader_failed.wait_till() => {
                    Event::CloseLeader(true)
                }
                _ = self.timers.ensure_leader.wait_till() => {
                    Event::EnsureLeader
                }
                _ = self.timers.connect_to_leader.wait_till() => {
                    Event::ConnectToLeader
                }
                _ = self.timers.send_ping.wait_till() => {
                    Event::SendLeaderPing
                }
                _ = self.timers.pong_timeout.wait_till() => {
                    Event::CloseLeader(true)
                }
            };

            self.handle_event(event).await;
        }
    }

    pub async fn handle_event(&mut self, event: Event<I, D>) {
        match event {
            Event::CloseLeader(failed) => {
                if let Some(follow) = self.follow_state.take() {
                    follow.cancel.cancel();
                    if failed {
                        if let Some(peer) = self.peers.get_mut(&follow.remote) {
                            peer.last_failure_epoch = now_ms();
                        }
                    }
                }

                if self.leader != self.local {
                    self.timers.connect_to_leader.now();
                }
            }
            Event::EnsureLeader => {
                let Some(follow) = &self.follow_state else { return };

                if follow.to_leader.try_send(SyncRequest::WhoisLeader).is_err() {
                    self.metrics.error_failed_to_ask_leader.inc();
                    self.timers.ensure_leader.set(Duration::from_secs(1));
                }
            }
            Event::ConnectToLeader => {
                if self.leader == self.local {
                    return;
                }

                let connect_target = 'find: {
                    /* try to connect to leader unless we failed within the last 5m */
                    if let Some(target) = self.peers.get(&self.leader) {
                        if 300_000 < now_ms() - target.last_failure_epoch {
                            break 'find self.leader;
                        }
                    } else {
                        break 'find self.leader;
                    }

                    let best_option = self.peers.iter().min_by(|(_, a), (_, b)| {
                        let oldest = now_ms() - 300_000;

                        /* round to ~half minute */
                        let a_fail = a.last_failure_epoch.max(oldest) >> 14;
                        let b_fail = a.last_failure_epoch.max(oldest) >> 14;

                        match a_fail.cmp(&b_fail) {
                            Ordering::Equal => {}
                            other => return other,
                        }

                        /* round by ~10ms */
                        let a_latency = a.latency.as_ref().map(|v| v.latency_ms).unwrap_or(300) >> 3;
                        let b_latency = b.latency.as_ref().map(|v| v.latency_ms).unwrap_or(300) >> 3;

                        a_latency.cmp(&b_latency)
                    });

                    match best_option {
                        Some((addr, _)) => *addr,
                        None => self.leader
                    }
                };

                // self.io.connect(remote)
            }
            Event::SendLeaderPing => {
                if let Some(follow) = &self.follow_state {
                    if follow.to_leader.try_send(SyncRequest::Ping(now_ms())).is_err() {
                        self.metrics.error_failed_to_ping_leader.inc();
                        self.timers.send_ping.set_earlier(Duration::from_secs(2));
                    }
                }
            }
            Event::LeaderMessage(msg) => {
                let follow = self.follow_state.as_mut().panic("follow_state missing");

                match msg {
                    SyncResponse::Pong(pong) => {
                        let now = now_ms();
                        let latency = Latency {
                            latency_ms: now.max(pong) - pong,
                            last_pong_epoch: now,
                        };

                        match self.peers.entry(follow.remote) {
                            hash_map::Entry::Vacant(v) => {
                                v.insert(DiscoveredPeer {
                                    latency: Some(latency),
                                    last_failure_epoch: 0,
                                    last_success_epoch: 0,
                                });
                            },
                            hash_map::Entry::Occupied(o) => {
                                o.into_mut().latency.replace(latency);
                            }
                        }

                        self.timers.send_ping.set(Duration::from_secs(5));
                        self.timers.pong_timeout.set(Duration::from_secs(120));
                    }
                    SyncResponse::FreshState(state) => {
                        if follow.feed_updater.is_some() {
                            tracing::error!("already in session with leader but got fresh state");
                            self.timers.leader_failed.now();
                            return;
                        }

                        let (action_tx, mut action_rx) = channel(1024);
                        let (authority_tx, authority_rx) = channel(1024);
                        let seq = state.sequence();

                        self.updater.follow(action_tx, NewFollowState {
                            state,
                            authority_rx: SequencedReceiver::new(seq, authority_rx),
                        }.try_into_valid().panic("could not create valid follow state")).await;

                        follow.feed_updater = Some(SequencedSender::new(seq, authority_tx));

                        /* forward actions to leader */
                        {
                            let to_leader = follow.to_leader.clone();
                            tokio::spawn(follow.cancel.clone().run_until_cancelled_owned(async move {
                                while let Some((source, action)) = action_rx.recv().await {
                                    if to_leader.send(SyncRequest::Action { source, action }).await.is_err() {
                                        break;
                                    }
                                }

                                tokio::task::yield_now().await;
                                tracing::error!("forwarding action to leader task stopped without cancel");
                            }));
                        }

                        tracing::info!("Fresh state setup");
                    }
                    SyncResponse::RecoveryAccepted(seq) => {
                        if follow.feed_updater.is_some() {
                            tracing::error!("already in session with leader but got fresh state");
                            self.timers.leader_failed.now();
                            return;
                        }

                        let (action_tx, mut action_rx) = channel(1024);
                        let (authority_tx, authority_rx) = channel(1024);

                        let success = self.updater.try_follow(action_tx, FollowTarget {
                            leader_state_check: None,
                            authority_rx: SequencedReceiver::new(seq, authority_rx),
                        }).await;

                        if !success {
                            tracing::error!("failed to recover connection from leader");
                            self.timers.leader_failed.now();
                            return;
                        }

                        follow.feed_updater = Some(SequencedSender::new(seq, authority_tx));

                        /* forward actions to leader */
                        {
                            let to_leader = follow.to_leader.clone();
                            tokio::spawn(follow.cancel.clone().run_until_cancelled_owned(async move {
                                while let Some((source, action)) = action_rx.recv().await {
                                    if to_leader.send(SyncRequest::Action { source, action }).await.is_err() {
                                        break;
                                    }
                                }

                                tokio::task::yield_now().await;
                                tracing::error!("forwarding action to leader task stopped without cancel");
                            }));
                        }

                        tracing::info!("Fresh state setup");
                    }
                    SyncResponse::AuthorityAction(seq, action) => {
                        let Some(updater) = follow.feed_updater.as_mut() else {
                            tracing::error!("got AuthorityAction but not following leader yet");
                            self.timers.leader_failed.now();
                            return;
                        };

                        if let Err(error) = updater.safe_send(seq, action).await {
                            tracing::error!(?error, "leader sent invalid sequence");
                            self.timers.leader_failed.now();
                        }
                    }
                    SyncResponse::Leader(leader) => {
                        if leader == self.leader {
                            self.timers.confirm_leader_timeout.clear();
                        }
                    }
                    SyncResponse::Peers(peers) => {
                        for peer in peers {
                            if let hash_map::Entry::Vacant(v) = self.peers.entry(peer) {
                                v.insert(DiscoveredPeer {
                                    latency: None,
                                    last_failure_epoch: 0,
                                    last_success_epoch: 0,
                                });
                            }
                        }
                    }
                }
            }
            Event::SetLeader(leader) => {
                let changed = std::mem::replace(&mut self.leader, leader) != leader;
                if !changed {
                    return;
                }

                if self.local == self.leader {
                    if let Some(state) = self.follow_state.take() {
                        state.cancel.cancel();
                    }
                    tracing::info!("self set as leader");
                    self.updater.lead().await;
                } else if self.follow_state.is_some() {
                    self.timers.ensure_leader.now();
                    self.timers.confirm_leader_timeout.set(Duration::from_secs(10));
                } else {
                    tracing::info!("need leader connection");
                    self.timers.connect_to_leader.set_earlier(Duration::from_secs(1));
                }
            }
            Event::ClientMessage(ClientMessage { client, msg, send }) => {
                if client.cancel.is_cancelled() {
                    return;
                }

                match msg {
                    SyncRequest::Ping(num) => {
                        self.metrics.client_req_ping.inc();

                        if send.try_send(SyncResponse::Pong(num)).is_err() {
                            self.metrics.client_send_dropped.inc();
                        }
                    }
                    SyncRequest::SendMePeers => {
                        self.metrics.client_req_send_me_peers.inc();

                        let Ok(permit) = send.try_reserve() else {
                            self.metrics.client_send_dropped.inc();
                            return;
                        };

                        let now = now_ms();

                        let peers = self.peers.iter().filter_map(|(key, value)| {
                            let last_seen = value.latency.as_ref()?.last_pong_epoch;
                            if now < last_seen || 30_000 < now - last_seen {
                                return None;
                            }
                            Some(*key)
                        }).collect::<Vec<_>>();

                        permit.send(SyncResponse::Peers(peers));
                    }
                    SyncRequest::NoticePeers(peers) => {
                        self.metrics.client_req_notice_peers.inc();

                        for peer in peers {
                            if let hash_map::Entry::Vacant(v) = self.peers.entry(peer) {
                                v.insert(DiscoveredPeer {
                                    latency: None,
                                    last_failure_epoch: 0,
                                    last_success_epoch: 0,
                                });
                            }
                        }
                    }
                    SyncRequest::WhoisLeader => {
                        self.metrics.client_req_whois_leader.inc();

                        if send.try_send(SyncResponse::Leader(self.leader)).is_err() {
                            self.metrics.client_send_dropped.inc();
                        }
                    }
                    SyncRequest::Action { source, action } => {
                        self.metrics.client_req_action.inc();

                        if self.updater.action_tx.try_send((source, action)).is_err() {
                            self.metrics.client_action_dropped.inc();
                        }
                    }
                    SyncRequest::SubscribeRecovery(recovery) => {
                        self.metrics.client_req_recover.inc();

                        let Some(mut follow_target) = self.updater.add_subscriber(recovery).await else {
                            self.metrics.client_recovery_fails.inc();

                            if self.client_msg_tx.try_send(ClientMessage { client: client.clone(), msg: SyncRequest::SubscribeFresh, send }).is_err() {
                                self.metrics.event_queue_fail.inc();
                                client.cancel.cancel();
                            }

                            return;
                        };

                        let exclusive = client.mode.compare_exchange(
                            MODE_CONNECTING, MODE_CONNECTED,
                            std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst
                        ).is_ok();

                        if !exclusive {
                            self.metrics.client_dual_connect_error.inc();
                            return;
                        }

                        tokio::spawn(client.cancel.clone().run_until_cancelled_owned(async move {
                            if send.send(SyncResponse::RecoveryAccepted(follow_target.authority_rx.next_seq())).await.is_err() {
                                client.cancel.cancel();
                                return;
                            }

                            while let Some(msg) = follow_target.authority_rx.recv().await {
                                if send.send(SyncResponse::AuthorityAction(msg.0, msg.1)).await.is_err() {
                                    break;
                                }
                            }

                            client.cancel.cancel();
                        }));
                    }
                    SyncRequest::SubscribeFresh => {
                        self.metrics.client_req_fresh.inc();

                        let exclusive = client.mode.compare_exchange(
                            MODE_CONNECTING, MODE_CONNECTED,
                            std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst
                        ).is_ok();

                        if !exclusive {
                            self.metrics.client_dual_connect_error.inc();
                            return;
                        }

                        let Some(details) = self.updater.add_fresh_subscriber().await else {
                            self.metrics.client_fresh_fails.inc();
                            tracing::error!("Failed to add fresh client");
                            client.cancel.cancel();
                            return;
                        };

                        let NewFollowState {
                            state,
                            mut authority_rx,
                        } = details.into_inner();

                        tokio::spawn(async move {
                            if send.send(SyncResponse::FreshState(state)).await.is_err() {
                                client.cancel.cancel();
                                return;
                            }

                            while let Some(msg) = authority_rx.recv().await {
                                if send.send(SyncResponse::AuthorityAction(msg.0, msg.1)).await.is_err() {
                                    break;
                                }
                            }

                            client.cancel.cancel();
                        });
                    }
                }
            }
        }
    }

    pub fn add_client(&mut self, client: SyncConnection<I>) {
        let state = Arc::new(ClientState {
            source_id: client.remote,
            mode: AtomicU64::new(MODE_CONNECTING),
            cancel: CancellationToken::new(),
        });

        let mut from_client = {
            let (from_client_tx, from_client_rx) = channel::<SyncRequest<I, D>>(1024);

            tokio::spawn(state.cancel.clone().run_until_cancelled_owned(
                ReadChannel::<I, _> {
                    input: client.read,
                    output: from_client_tx,
                    settings: self.net_settings.clone(),
                }.start()
            ));
            
            from_client_rx
        };

        let to_client = {
            let (to_client_tx, to_client_rx) = channel::<SyncResponse<I, D>>(1024);

            tokio::spawn(state.cancel.clone().run_until_cancelled_owned(
                WriteChannel::<I, _> {
                    input: to_client_rx,
                    output: client.write,
                    settings: self.net_settings.clone()
                }.start()
            ));

            to_client_tx
        };

        let event_tx = self.client_msg_tx.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            metrics.clients_connected.inc();

            state.cancel.clone().run_until_cancelled_owned(async move {
                while let Some(msg) = from_client.recv().await {
                    if event_tx.send(ClientMessage { client: state.clone(), msg, send: to_client.clone() }).await.is_err() {
                        tracing::warn!("SyncManager worker closed, stopping read from client");
                        break;
                    }
                }

                state.cancel.cancel();
            }).await;

            metrics.clients_connected.dec();
        });
    }
}

#[derive(Debug, Default)]
struct TimerOpt {
    expires_at: Option<Instant>,
    trigger: bool,
}

impl TimerOpt {
    fn now(&mut self) {
        self.trigger = true;
    }

    fn set(&mut self, wait: Duration) {
        self.expires_at = Some(Instant::now() + wait);
    }

    fn set_earlier(&mut self, wait: Duration) {
        let updated = Instant::now() + wait;
        self.expires_at = match self.expires_at {
            Some(opt) if opt < updated => Some(opt),
            _ => Some(updated),
        };
    }

    fn clear(&mut self) {
        self.expires_at.take();
    }

    async fn wait_till(&mut self) {
        if self.trigger {
            return;
        }

        if let Some(expires_at) = self.expires_at {
            let _ = tokio::time::sleep_until(expires_at).await;
            self.expires_at.take();
        } else {
            poll_fn(|_| Poll::<()>::Pending).await;
        }
    }
}

pub const MODE_CONNECTING: u64 = 0;
pub const MODE_CONNECTED: u64 = 1;

pub struct ClientState<S: SourceId> {
    source_id: S,
    mode: AtomicU64,
    cancel: CancellationToken,
}

struct DiscoveredPeer {
    latency: Option<Latency>,
    last_failure_epoch: u64,
    last_success_epoch: u64,
}

struct Latency {
    last_pong_epoch: u64,
    latency_ms: u64,
}
