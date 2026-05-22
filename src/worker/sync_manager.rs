use std::{
    collections::{hash_map, HashMap},
    fmt::Debug,
    sync::{atomic::AtomicU64, Arc},
    task::Poll,
    time::Duration,
};

use arc_metrics::{IntCounter, IntGauge};
use futures_util::future::poll_fn;
use message_encoding::MessageEncoding;
use sequenced_broadcast::{SequencedBroadcastSettings, SequencedReceiver, SequencedSender};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{
    net::{
        io::{SyncConnection, SyncIO},
        message_channels::{NetIoSettings, ReadChannel, WriteChannel},
        messages::{PeerInfo, SentinelConnectivity, SentinelVote, SyncRequest, SyncResponse},
    },
    recoverable_state::{RecoverableState, RecoverableStateAction, SourceId},
    state::{DeterministicState, SharedState},
    utils::{now_ms, LogHelper, PanicHelper},
    worker::sync_updater::FollowTarget,
};

use super::sync_updater::{NewFollowState, SyncUpdater};

pub struct SyncManager<I: SyncIO, D: DeterministicState> {
    action_tx: Sender<D::Action>,
    shared: SharedState<RecoverableState<I::Address, D>>,
    control_tx: Sender<ControlMessage<I>>,
    metrics: Arc<SyncManagerMetrics>,
}

impl<I: SyncIO, D: DeterministicState> SyncManager<I, D>
where
    D: MessageEncoding,
    D::AuthorityAction: MessageEncoding + Clone,
    D::Action: MessageEncoding,
{
    pub fn new(
        io: Arc<I>,
        local: I::Address,
        state: D,
        settings: SyncMangerSettings<I::Address>,
    ) -> Self {
        let (control_tx, control_rx) = channel(256);
        let worker = SyncManagerWorker::new(io, local, control_rx, state, settings);

        let shared = worker.updater.state().clone();
        let action_tx = worker.updater.action_tx();
        let metrics = worker.metrics.clone();

        tokio::spawn(
            ClientAcceptor {
                metrics: metrics.clone(),
                msg_tx: worker.client_msg_tx.clone(),
                net_settings: worker.net_settings.clone(),
                io: worker.io.clone(),
                is_sentinel: worker.settings_is_sentinel,
            }
            .start()
            .instrument(tracing::Span::current()),
        );

        tokio::spawn(worker.start().instrument(tracing::Span::current()));

        SyncManager {
            action_tx,
            shared,
            control_tx,
            metrics,
        }
    }

    pub fn sync_metrics_ref(&self) -> &Arc<SyncManagerMetrics> {
        &self.metrics
    }

    pub fn shared(&self) -> SharedState<RecoverableState<I::Address, D>> {
        self.shared.clone()
    }

    pub fn action_tx(&self) -> Sender<D::Action> {
        self.action_tx.clone()
    }

    pub async fn set_leader(&self, leader: I::Address) {
        self.control_tx
            .send(ControlMessage::SetLeader(leader))
            .await
            .panic("worker closed");
    }
}

enum ControlMessage<I: SyncIO> {
    SetLeader(I::Address),
}

struct SyncManagerWorker<I: SyncIO, D: DeterministicState> {
    io: Arc<I>,

    local: I::Address,
    leader: I::Address,

    last_connect_attempt: Option<I::Address>,
    settings_is_sentinel: bool,
    sentinel_settings: SentinelSettings,

    metrics: Arc<SyncManagerMetrics>,

    follow_state: Option<FollowState<I, D>>,

    updater: SyncUpdater<I::Address, D>,
    peers: HashMap<I::Address, DiscoveredPeer<I, D>>,
    sentinel_links: HashMap<I::Address, SentinelLink<I, D>>,
    sentinel_stats: HashMap<I::Address, SentinelConnectionStats>,
    peer_sentinel_reports: HashMap<I::Address, TimedPeerSentinelReport<I::Address>>,
    sentinel_votes: HashMap<I::Address, TimedSentinelVote<I::Address>>,

    control_rx: Receiver<ControlMessage<I>>,
    client_msg_tx: Sender<ClientMessage<I, D>>,
    client_msg_rx: Receiver<ClientMessage<I, D>>,
    conn_tx: Sender<ConnectionUpdate<I, D>>,
    conn_rx: Receiver<ConnectionUpdate<I, D>>,

    timers: Timers,

    connect_timeout: Duration,
    receive_state_timeout: Duration,
    net_settings: NetIoSettings,

    waiting_for_connection_update: bool,
}

#[repr(usize)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Timer {
    SendVerifyLeader,
    RequireVerifyLeader,

    RejectPeer,
    ConnectToPeer,

    RequireState,
    BroadcastPeers,
    ConnectSentinels,
    ReportSentinelConnectivity,
    EvaluateSentinelElection,
}

#[derive(Default)]
struct Timers {
    next_scan: usize,
    timers: Vec<TimerOpt>,
}

impl Debug for Timers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Timers {{ active: {:?} }}", self.active())
    }
}

impl Timers {
    pub fn get(&mut self, timer: Timer) -> &mut TimerOpt {
        let pos = timer as usize;
        while self.timers.len() <= pos {
            self.timers.push(TimerOpt::default());
        }
        &mut self.timers[pos]
    }

    pub fn active(&self) -> Vec<Timer> {
        let mut timers = Vec::new();
        for (pos, state) in self.timers.iter().enumerate() {
            if state.is_set() {
                timers.push(unsafe { std::mem::transmute::<usize, Timer>(pos) });
            }
        }
        timers
    }

    pub async fn wait(&mut self) -> Timer {
        let done_timer_pos = 'triggered: {
            let mut next = Option::<(Instant, usize)>::None;

            for _ in 0..self.timers.len() {
                let pos = self.next_scan % self.timers.len();
                let timer = &mut self.timers[pos];
                self.next_scan += 1;

                if timer.trigger {
                    break 'triggered pos;
                }

                let Some(opt) = timer.expires_at else {
                    continue;
                };

                next = match next {
                    Some((time, pos)) if time < opt => Some((time, pos)),
                    _ => Some((opt, pos)),
                };
            }

            match next {
                None => {
                    poll_fn(|_| Poll::<()>::Pending).await;
                    unreachable!()
                }
                Some((next, pos)) => {
                    tokio::time::sleep_until(next).await;
                    pos
                }
            }
        };

        self.timers[done_timer_pos].clear();
        unsafe { std::mem::transmute::<usize, Timer>(done_timer_pos) }
    }
}

// #[derive(Debug, Default)]
// struct Timers {
//     ensure_leader: TimerOpt,
//     confirm_leader_timeout: TimerOpt,
//     leader_failed: TimerOpt,
//     send_ping: TimerOpt,
//     pong_timeout: TimerOpt,
//     connect_to_leader: TimerOpt,
//     receive_state_timeout: TimerOpt,
//     broadcast_peers: TimerOpt,
// }

struct FollowState<I: SyncIO, D: DeterministicState> {
    remote: I::Address,
    cancel: CancellationToken,
    leader_path: Vec<I::Address>,
    to_leader: Sender<SyncRequest<I, D>>,
    from_leader: Receiver<SyncResponse<I, D>>,
    feed_updater: Option<SequencedSender<RecoverableStateAction<I::Address, D::AuthorityAction>>>,
}

struct SentinelLink<I: SyncIO, D: DeterministicState> {
    cancel: CancellationToken,
    send: Sender<SyncRequest<I, D>>,
}

#[derive(Clone, Debug, Default)]
struct SentinelConnectionStats {
    is_connected: bool,
    disconnect_epochs: Vec<u64>,
    last_update_epoch: u64,
}

impl SentinelConnectionStats {
    fn disconnects_in_past_hour(&mut self) -> u32 {
        let cutoff = now_ms().saturating_sub(3_600_000);
        self.disconnect_epochs.retain(|epoch| cutoff <= *epoch);
        self.disconnect_epochs.len() as u32
    }

    fn mark_connected(&mut self) {
        self.is_connected = true;
        self.last_update_epoch = now_ms();
    }

    fn mark_disconnected(&mut self) {
        self.is_connected = false;
        let now = now_ms();
        self.last_update_epoch = now;
        self.disconnect_epochs.push(now);
        self.disconnects_in_past_hour();
    }
}

struct TimedPeerSentinelReport<A> {
    reports: Vec<SentinelConnectivity<A>>,
    epoch: u64,
}

struct TimedSentinelVote<A> {
    vote: SentinelVote<A>,
    epoch: u64,
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

enum Event<I: SyncIO, D: DeterministicState> {
    ClientMessage(ClientMessage<I, D>),
    Control(ControlMessage<I>),
    ConnectionUpdate(ConnectionUpdate<I, D>),
    Timer(Timer),
    LeaderMessage(SyncResponse<I, D>),
    Shutdown,
}

impl<I: SyncIO, D: DeterministicState> Debug for Event<I, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ClientMessage(ClientMessage { client, msg, .. }) => write!(
                f,
                "ClientMessage {{ client: {:?}, message: {:?} }}",
                client.source_id, msg
            ),
            Self::Control(ControlMessage::SetLeader(leader)) => {
                write!(f, "SetLeader({:?})", leader)
            }
            Self::ConnectionUpdate(ConnectionUpdate::UpdatedFollow(FollowState {
                remote, ..
            })) => write!(
                f,
                "ConnectionUpdate::UpdatedFollow {{ remote: {:?} }}",
                remote
            ),
            Self::ConnectionUpdate(ConnectionUpdate::ConnectFailed(addr)) => {
                write!(f, "ConnectionUpdate::ConnectFailed({:?})", addr)
            }
            Self::ConnectionUpdate(ConnectionUpdate::SentinelConnected { address, .. }) => {
                write!(f, "ConnectionUpdate::SentinelConnected({:?})", address)
            }
            Self::ConnectionUpdate(ConnectionUpdate::SentinelDisconnected(address)) => {
                write!(f, "ConnectionUpdate::SentinelDisconnected({:?})", address)
            }
            Self::Timer(timer) => write!(f, "Timer({:?})", timer),
            Self::LeaderMessage(SyncResponse::NodeInfo { is_sentinel }) => {
                write!(f, "LeaderMessage::NodeInfo({})", is_sentinel)
            }
            Self::LeaderMessage(SyncResponse::LeaderPath(path)) => {
                write!(f, "LeaderMessage::LeaderPath({:?})", path)
            }
            Self::LeaderMessage(SyncResponse::Pong(num)) => {
                write!(f, "LeaderMessage::Pong({})", num)
            }
            Self::LeaderMessage(SyncResponse::FreshState(state)) => {
                write!(f, "LeaderMessage::FreshState(seq: {})", state.accept_seq())
            }
            Self::LeaderMessage(SyncResponse::RecoveryAccepted(seq)) => {
                write!(f, "LeaderMessage::RecoveryAccepted(seq: {})", seq)
            }
            Self::LeaderMessage(SyncResponse::Peers(peers)) => {
                write!(f, "LeaderMessage::Peers(count: {})", peers.len())
            }
            Self::LeaderMessage(SyncResponse::PeerInfo(peers)) => {
                write!(f, "LeaderMessage::PeerInfo(count: {})", peers.len())
            }
            Self::LeaderMessage(SyncResponse::AuthorityAction(seq, _)) => {
                write!(f, "LeaderMessage::AuthorityAction(seq: {})", seq)
            }
            Self::Shutdown => write!(f, "Shutdown"),
            // _ => write!(f, "TODO"),
        }
    }
}

enum ConnectionUpdate<I: SyncIO, D: DeterministicState> {
    ConnectFailed(I::Address),
    UpdatedFollow(FollowState<I, D>),
    SentinelConnected {
        address: I::Address,
        send: Sender<SyncRequest<I, D>>,
        cancel: CancellationToken,
    },
    SentinelDisconnected(I::Address),
}

struct ClientMessage<I: SyncIO, D: DeterministicState> {
    client: Arc<ClientState<I::Address>>,
    msg: SyncRequest<I, D>,
    send: Sender<SyncResponse<I, D>>,
}

pub struct SyncMangerSettings<A> {
    pub broadcast: SequencedBroadcastSettings,
    pub net_io: NetIoSettings,
    pub connect_timeout: Duration,
    pub receive_state_timeout: Duration,
    pub initial_peers: Vec<A>,
    pub is_sentinel: bool,
    pub sentinel: SentinelSettings,
}

#[derive(Clone, Debug)]
pub struct SentinelSettings {
    pub report_interval: Duration,
    pub election_interval: Duration,
    pub unhealthy_disconnect_threshold: u32,
    pub stale_report_after: Duration,
}

impl Default for SentinelSettings {
    fn default() -> Self {
        Self {
            report_interval: Duration::from_secs(15),
            election_interval: Duration::from_secs(5),
            unhealthy_disconnect_threshold: 3,
            stale_report_after: Duration::from_secs(90),
        }
    }
}

impl<A> Default for SyncMangerSettings<A> {
    fn default() -> Self {
        Self {
            broadcast: Default::default(),
            net_io: Default::default(),
            connect_timeout: Duration::from_secs(8),
            receive_state_timeout: Duration::from_secs(80),
            initial_peers: Vec::new(),
            is_sentinel: false,
            sentinel: Default::default(),
        }
    }
}

impl<I: SyncIO, D: DeterministicState> SyncManagerWorker<I, D>
where
    D: MessageEncoding,
    D::AuthorityAction: MessageEncoding + Clone,
    D::Action: MessageEncoding,
{
    fn new(
        io: Arc<I>,
        local: I::Address,
        control_rx: Receiver<ControlMessage<I>>,
        state: D,
        settings: SyncMangerSettings<I::Address>,
    ) -> Self {
        let (client_msg_tx, client_msg_rx) = channel(2048);
        let (conn_tx, conn_rx) = channel(256);
        let is_sentinel = settings.is_sentinel;
        let sentinel_settings = settings.sentinel.clone();
        let peers = settings
            .initial_peers
            .into_iter()
            .filter(|addr| addr != &local)
            .map(|addr| (addr, DiscoveredPeer::new()))
            .collect();

        SyncManagerWorker {
            io,
            local,
            leader: local,
            last_connect_attempt: None,
            settings_is_sentinel: is_sentinel,
            sentinel_settings,
            metrics: Arc::new(SyncManagerMetrics::default()),
            follow_state: None,
            control_rx,
            client_msg_tx,
            client_msg_rx,
            conn_tx,
            conn_rx,
            peers,
            sentinel_links: HashMap::new(),
            sentinel_stats: HashMap::new(),
            peer_sentinel_reports: HashMap::new(),
            sentinel_votes: HashMap::new(),
            updater: SyncUpdater::new(local, state, settings.broadcast),
            timers: Timers::default(),
            connect_timeout: settings.connect_timeout,
            receive_state_timeout: settings.receive_state_timeout,
            net_settings: settings.net_io,
            waiting_for_connection_update: false,
        }
        .with_initial_sentinel_timers()
    }

    fn with_initial_sentinel_timers(mut self) -> Self {
        self.timers
            .get(Timer::ConnectSentinels)
            .set(Duration::from_secs(1));
        self.timers
            .get(Timer::ReportSentinelConnectivity)
            .set(self.sentinel_settings.report_interval);

        if self.settings_is_sentinel {
            self.sentinel_stats
                .entry(self.local)
                .or_default()
                .mark_connected();
            self.timers
                .get(Timer::EvaluateSentinelElection)
                .set(self.sentinel_settings.election_interval);
        }

        self
    }

    fn peer_entry(&mut self, address: I::Address) -> &mut DiscoveredPeer<I, D> {
        self.peers
            .entry(address)
            .or_insert_with(DiscoveredPeer::new)
    }

    fn mark_peer_sentinel(&mut self, address: I::Address, is_sentinel: bool) {
        if address == self.local {
            return;
        }

        let peer = self.peer_entry(address);
        if is_sentinel && !peer.is_sentinel {
            tracing::info!(peer = ?address, "discovered sentinel");
        }
        peer.is_sentinel |= is_sentinel;

        if peer.is_sentinel {
            self.sentinel_stats.entry(address).or_default();
            self.timers
                .get(Timer::ConnectSentinels)
                .set_earlier(Duration::from_secs(1));
        }
    }

    fn apply_peer_info(&mut self, peers: Vec<PeerInfo<I::Address>>) {
        for peer in peers {
            self.mark_peer_sentinel(peer.address, peer.is_sentinel);
        }
    }

    fn peer_infos(&self) -> Vec<PeerInfo<I::Address>> {
        let mut peers = self
            .peers
            .iter()
            .map(|(address, peer)| PeerInfo {
                address: *address,
                is_sentinel: peer.is_sentinel,
            })
            .collect::<Vec<_>>();

        peers.push(PeerInfo {
            address: self.local,
            is_sentinel: self.settings_is_sentinel,
        });
        peers
    }

    fn known_sentinels(&self) -> Vec<I::Address> {
        let mut sentinels = self
            .peers
            .iter()
            .filter(|(_, peer)| peer.is_sentinel)
            .map(|(address, _)| *address)
            .collect::<Vec<_>>();

        if self.settings_is_sentinel {
            sentinels.push(self.local);
        }

        sentinels
    }

    fn connectivity_report(&mut self) -> Vec<SentinelConnectivity<I::Address>> {
        self.known_sentinels()
            .into_iter()
            .filter(|sentinel| *sentinel != self.local)
            .map(|sentinel| {
                let stats = self.sentinel_stats.entry(sentinel).or_default();
                SentinelConnectivity {
                    sentinel,
                    is_connected: stats.is_connected,
                    disconnects_in_past_hour: stats.disconnects_in_past_hour(),
                }
            })
            .collect()
    }

    fn current_state_vote(&self, candidate: I::Address) -> SentinelVote<I::Address> {
        let details = self.updater.state.read().details().clone();
        SentinelVote {
            observed_master: self.leader,
            observed_state_id: details.id,
            candidate,
            candidate_state_accept_seq: details.state_accept_seq,
            candidate_recover_accept_seq: details.recover_accept_seq,
        }
    }

    fn encoded_addr_key(address: &I::Address) -> Vec<u8> {
        let mut out = Vec::new();
        address.write_to(&mut out).unwrap();
        out
    }

    fn choose_sentinel_candidate(&self) -> Option<I::Address> {
        let mut best: Option<(I::Address, u64, u64, Vec<u8>)> = None;
        let local_details = self.updater.state.read().details().clone();

        for sentinel in self.known_sentinels() {
            if sentinel == self.leader {
                continue;
            }

            let (state_seq, recover_seq) = if sentinel == self.local {
                (
                    local_details.state_accept_seq,
                    local_details.recover_accept_seq,
                )
            } else if let Some(vote) = self.sentinel_votes.get(&sentinel) {
                (
                    vote.vote.candidate_state_accept_seq,
                    vote.vote.candidate_recover_accept_seq,
                )
            } else {
                (
                    local_details.state_accept_seq,
                    local_details.recover_accept_seq,
                )
            };

            let key = Self::encoded_addr_key(&sentinel);
            let replace = match &best {
                None => true,
                Some((_, best_state, best_recover, best_key)) => {
                    state_seq > *best_state
                        || (state_seq == *best_state && recover_seq > *best_recover)
                        || (state_seq == *best_state
                            && recover_seq == *best_recover
                            && key < *best_key)
                }
            };

            if replace {
                best = Some((sentinel, state_seq, recover_seq, key));
            }
        }

        best.map(|(sentinel, _, _, _)| sentinel)
    }

    fn local_master_unhealthy(&mut self) -> bool {
        if self.leader == self.local {
            return false;
        }

        if self.updater.is_following() {
            return false;
        }

        let stale_after = self.sentinel_settings.stale_report_after.as_millis() as u64;
        let now = now_ms();
        let peer_report_unhealthy = self.peer_sentinel_reports.values().any(|report| {
            now.saturating_sub(report.epoch) <= stale_after
                && report.reports.iter().any(|item| {
                    item.sentinel == self.leader
                        && (!item.is_connected
                            || self.sentinel_settings.unhealthy_disconnect_threshold
                                <= item.disconnects_in_past_hour)
                })
        });

        let leader_stats = self.peers.get(&self.leader);
        peer_report_unhealthy
            || leader_stats
                .map(|stats| {
                    0 < stats.last_peer_failure_epoch
                        || stats
                            .latency
                            .as_ref()
                            .map(|latency| {
                                (self.sentinel_settings.stale_report_after.as_millis() as u64)
                                    < now_ms().saturating_sub(latency.last_pong_epoch)
                            })
                            .unwrap_or(true)
                })
                .unwrap_or(true)
    }

    async fn maybe_elect_sentinel_master(&mut self) {
        if !self.settings_is_sentinel || !self.local_master_unhealthy() {
            return;
        }

        let known_sentinels = self.known_sentinels();
        if known_sentinels.len() <= 1 {
            return;
        }

        let Some(candidate) = self.choose_sentinel_candidate() else {
            return;
        };

        let vote = self.current_state_vote(candidate);
        self.sentinel_votes.insert(
            self.local,
            TimedSentinelVote {
                vote: vote.clone(),
                epoch: now_ms(),
            },
        );

        for link in self.sentinel_links.values() {
            let _ = link.send.try_send(SyncRequest::SentinelVote(vote.clone()));
        }

        let required_votes = known_sentinels.len() / 2 + 1;
        let stale_after = self.sentinel_settings.stale_report_after.as_millis() as u64;
        let now = now_ms();
        let votes = self
            .sentinel_votes
            .values()
            .filter(|timed| {
                now.saturating_sub(timed.epoch) <= stale_after
                    && timed.vote.observed_master == self.leader
                    && timed.vote.observed_state_id == vote.observed_state_id
                    && timed.vote.candidate == candidate
            })
            .count();

        if votes < required_votes {
            return;
        }

        if candidate == self.local {
            self.set_leader_inner(self.local).await;
        } else {
            self.set_leader_inner(candidate).await;
        }
    }

    async fn set_leader_inner(&mut self, leader: I::Address) {
        let changed = std::mem::replace(&mut self.leader, leader) != leader;
        if !changed {
            tracing::info!(?leader, "ignore set leader, no change");
            return;
        }

        if self.local == self.leader {
            tracing::info!("self set as leader");

            self.timers.get(Timer::ConnectToPeer).clear();
            self.timers.get(Timer::RequireState).clear();
            self.timers.get(Timer::SendVerifyLeader).clear();
            self.timers.get(Timer::RequireVerifyLeader).clear();

            if let Some(state) = self.follow_state.take() {
                state.cancel.cancel();
            }

            self.updater.lead().await;
        } else if self.follow_state.is_some() {
            tracing::info!("leader changed but connected to peer already, queue confirm request");

            self.timers
                .get(Timer::SendVerifyLeader)
                .set_earlier(Duration::from_secs(2));
            self.timers
                .get(Timer::RequireVerifyLeader)
                .set(Duration::from_secs(10));
        } else {
            self.updater.go_offline().await;

            tracing::info!("need leader connection");
            self.timers.get(Timer::ConnectToPeer).now();
        }
    }

    #[cfg(test)]
    fn assert_valid_state(&self) {
        if self.follow_state.is_some() {
            assert_ne!(self.leader, self.local);
            assert!(self.updater.is_following() || self.updater.is_offline());
        } else if self.updater.is_leading() {
            assert_eq!(self.leader, self.local);
        } else {
            assert!(self.updater.is_offline());
        }

        if self.updater.is_following() {
            assert!(self.follow_state.is_some());
        }

        if self.local == self.leader {
            assert!(self.updater.is_leading());
        }

        let active_timers = self.timers.active();

        if self.updater.is_following() {
            assert!(active_timers.contains(&Timer::SendVerifyLeader));
            assert!(active_timers.contains(&Timer::RequireVerifyLeader));
        }

        if self.updater.is_offline() {
            if self.follow_state.is_some() {
                if !active_timers.contains(&Timer::RejectPeer) {
                    assert!(active_timers.contains(&Timer::RequireState));
                    assert!(active_timers.contains(&Timer::SendVerifyLeader));
                    assert!(active_timers.contains(&Timer::RequireVerifyLeader));
                } else {
                    assert!(!active_timers.contains(&Timer::RequireState));
                    assert!(!active_timers.contains(&Timer::SendVerifyLeader));
                    assert!(!active_timers.contains(&Timer::RequireVerifyLeader));
                }
            } else {
                assert!(!active_timers.contains(&Timer::RequireState));
                assert!(!active_timers.contains(&Timer::SendVerifyLeader));
                assert!(!active_timers.contains(&Timer::RequireVerifyLeader));

                if self.waiting_for_connection_update {
                    assert!(!active_timers.contains(&Timer::ConnectToPeer));
                } else {
                    assert!(active_timers.contains(&Timer::ConnectToPeer));
                }
            }
        }

        if self.updater.is_leading() {
            assert!(!active_timers.contains(&Timer::RequireState));
            assert!(!active_timers.contains(&Timer::SendVerifyLeader));
            assert!(!active_timers.contains(&Timer::RequireVerifyLeader));
            assert!(self.follow_state.is_none());
        }

        if self.updater.is_following() || self.updater.is_leading() {
            assert!(!active_timers.contains(&Timer::ConnectToPeer));
            assert!(!active_timers.contains(&Timer::RequireState));
        }

        if self.waiting_for_connection_update {
            assert!(self.updater.is_offline());
        }
    }

    async fn next_event(&mut self) -> Event<I, D> {
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

        tokio::select! {
            msg_opt = self.client_msg_rx.recv() => {
                Event::ClientMessage(msg_opt.panic("client msg channel closed"))
            }
            conn_opt = self.conn_rx.recv() => {
                self.waiting_for_connection_update = false;
                Event::ConnectionUpdate(conn_opt.panic("conn channel closed"))
            }
            control_opt = self.control_rx.recv() => {
                if let Some(control) = control_opt {
                    Event::Control(control)
                } else {
                    Event::Shutdown
                }
            }
            leader_opt = read_from_leader => {
                match leader_opt {
                    None => {
                        tracing::warn!("leader read channel closed");
                        Event::Timer(Timer::RejectPeer)
                    },
                    Some(msg) => Event::LeaderMessage(msg)
                }
            }
            timer = self.timers.wait() => {
                Event::Timer(timer)
            }
            // _ = self.timers.confirm_leader_timeout.wait_till() => {
            //     Event::CloseLeader(true)
            // }
            // _ = self.timers.leader_failed.wait_till() => {
            //     Event::CloseLeader(true)
            // }
            // _ = self.timers.receive_state_timeout.wait_till() => {
            //     Event::CloseLeader(true)
            // }
            // _ = self.timers.ensure_leader.wait_till() => {
            //     Event::EnsureLeader
            // }
            // _ = self.timers.connect_to_leader.wait_till() => {
            //     Event::ConnectToLeader
            // }
            // _ = self.timers.send_ping.wait_till() => {
            //     Event::SendLeaderPing
            // }
            // _ = self.timers.pong_timeout.wait_till() => {
            //     Event::CloseLeader(true)
            // }
            // _ = self.timers.broadcast_peers.wait_till() => {
            //     Event::BroadcastPeers
            // }
        }
    }

    async fn start(mut self) {
        loop {
            tokio::task::yield_now().await;

            #[cfg(test)]
            self.assert_valid_state();

            let event = self.next_event().await;
            let shutdown = matches!(event, Event::Shutdown);

            if !matches!(
                event,
                Event::LeaderMessage(SyncResponse::AuthorityAction(..))
                    | Event::ClientMessage(ClientMessage {
                        msg: SyncRequest::Action { .. },
                        ..
                    })
            ) {
                tracing::info!("Next Event: {:?}", event);
            }

            self.handle_event(event).await;
            if shutdown {
                break;
            }
        }
    }

    async fn handle_event(&mut self, event: Event<I, D>) {
        match event {
            Event::Shutdown => {
                tracing::info!("Got shutdown event");
            }
            Event::Timer(Timer::BroadcastPeers) => {
                let peers = self.peers.keys().cloned().collect::<Vec<_>>();
                let peer_infos = self.peer_infos();

                let mut count = 0;
                for peer in self.peers.values() {
                    if let Some(send) = &peer.client_send {
                        count += 1;
                        let _ = send.try_send(SyncResponse::Peers(peers.clone()));
                        let _ = send.try_send(SyncResponse::PeerInfo(peer_infos.clone()));
                    }
                }

                tracing::info!("Broadcasted {} peers to {} peers", peers.len(), count);
                self.timers
                    .get(Timer::BroadcastPeers)
                    .set_earlier(Duration::from_secs(30));
            }
            Event::Timer(Timer::ConnectSentinels) => {
                let sentinels = self
                    .known_sentinels()
                    .into_iter()
                    .filter(|sentinel| *sentinel != self.local)
                    .filter(|sentinel| !self.sentinel_links.contains_key(sentinel))
                    .collect::<Vec<_>>();

                for sentinel in sentinels {
                    let io = self.io.clone();
                    let conn_tx = self.conn_tx.clone();
                    let conn_timeout = self.connect_timeout;
                    let net_settings = self.net_settings.clone();
                    let local = self.local;
                    let is_sentinel = self.settings_is_sentinel;

                    tokio::spawn(async move {
                        let cancel = CancellationToken::new();

                        let conn = match tokio::time::timeout(conn_timeout, io.connect(&sentinel))
                            .await
                        {
                            Ok(Ok(conn)) => conn,
                            Ok(Err(error)) => {
                                tracing::debug!(?error, sentinel = ?sentinel, "sentinel connect failed");
                                let _ = conn_tx
                                    .send(ConnectionUpdate::SentinelDisconnected(sentinel))
                                    .await;
                                return;
                            }
                            Err(_) => {
                                tracing::debug!(sentinel = ?sentinel, "sentinel connect timed out");
                                let _ = conn_tx
                                    .send(ConnectionUpdate::SentinelDisconnected(sentinel))
                                    .await;
                                return;
                            }
                        };

                        let mut from_server = {
                            let (from_server_tx, from_server_rx) =
                                channel::<SyncResponse<I, D>>(1024);
                            tokio::spawn(
                                cancel.clone().run_until_cancelled_owned(
                                    ReadChannel::<I, _> {
                                        input: conn.read,
                                        output: from_server_tx,
                                        settings: net_settings.clone(),
                                    }
                                    .start(),
                                ),
                            );
                            from_server_rx
                        };

                        let to_server = {
                            let (to_server_tx, to_server_rx) = channel::<SyncRequest<I, D>>(1024);
                            tokio::spawn(
                                cancel.clone().run_until_cancelled_owned(
                                    WriteChannel::<I, _> {
                                        input: to_server_rx,
                                        output: conn.write,
                                        settings: net_settings,
                                    }
                                    .start(),
                                ),
                            );
                            to_server_tx
                        };

                        if to_server.send(SyncRequest::MyAddress(local)).await.is_err()
                            || to_server
                                .send(SyncRequest::NodeInfo { is_sentinel })
                                .await
                                .is_err()
                        {
                            cancel.cancel();
                            let _ = conn_tx
                                .send(ConnectionUpdate::SentinelDisconnected(sentinel))
                                .await;
                            return;
                        }

                        let _ = to_server.try_send(SyncRequest::SendMePeerInfo);

                        if conn_tx
                            .send(ConnectionUpdate::SentinelConnected {
                                address: sentinel,
                                send: to_server,
                                cancel: cancel.clone(),
                            })
                            .await
                            .is_err()
                        {
                            cancel.cancel();
                            return;
                        }

                        while from_server.recv().await.is_some() {
                            tokio::task::yield_now().await;
                        }

                        cancel.cancel();
                        let _ = conn_tx
                            .send(ConnectionUpdate::SentinelDisconnected(sentinel))
                            .await;
                    });
                }

                self.timers
                    .get(Timer::ConnectSentinels)
                    .set_earlier(Duration::from_secs(10));
            }
            Event::Timer(Timer::ReportSentinelConnectivity) => {
                let report = self.connectivity_report();
                if !report.is_empty() {
                    for link in self.sentinel_links.values() {
                        let _ = link
                            .send
                            .try_send(SyncRequest::SentinelConnectivityReport(report.clone()));
                    }
                }

                self.timers
                    .get(Timer::ReportSentinelConnectivity)
                    .set(self.sentinel_settings.report_interval);
            }
            Event::Timer(Timer::EvaluateSentinelElection) => {
                self.maybe_elect_sentinel_master().await;
                self.timers
                    .get(Timer::EvaluateSentinelElection)
                    .set(self.sentinel_settings.election_interval);
            }
            Event::Timer(Timer::RejectPeer | Timer::RequireVerifyLeader | Timer::RequireState) => {
                self.timers.get(Timer::RejectPeer).clear();
                self.timers.get(Timer::SendVerifyLeader).clear();
                self.timers.get(Timer::RequireVerifyLeader).clear();
                self.timers.get(Timer::RequireState).clear();

                if let Some(follow) = self.follow_state.take() {
                    follow.cancel.cancel();

                    tracing::warn!(
                        remote = ?follow.remote,
                        "cancel follow, mark peer as failed"
                    );

                    match self.peers.entry(follow.remote) {
                        hash_map::Entry::Vacant(v) => {
                            v.insert(DiscoveredPeer {
                                latency: None,
                                last_peer_failure_epoch: now_ms(),
                                last_client_msg_epoch: 0,
                                client_send: None,
                                is_sentinel: false,
                            });
                        }
                        hash_map::Entry::Occupied(o) => {
                            o.into_mut().last_peer_failure_epoch = now_ms();
                        }
                    }
                }

                if self.leader != self.local {
                    self.updater.go_offline().await;
                    self.timers
                        .get(Timer::ConnectToPeer)
                        .set_earlier(Duration::from_secs(1));
                } else {
                    assert!(self.updater.is_leading());
                }
            }
            Event::Timer(Timer::SendVerifyLeader) => {
                let Some(follow) = &self.follow_state else {
                    return;
                };

                if follow
                    .to_leader
                    .try_send(SyncRequest::ShareLeaderPath)
                    .is_err()
                {
                    self.metrics.error_failed_to_ask_leader.inc();
                    self.timers
                        .get(Timer::RequireVerifyLeader)
                        .extend(Duration::from_secs(2), Duration::from_secs(30));
                }

                self.timers
                    .get(Timer::SendVerifyLeader)
                    .set_earlier(Duration::from_secs(30));
            }
            Event::Timer(Timer::ConnectToPeer) => {
                if self.leader == self.local {
                    return;
                }

                let mut opt_count = 0u64;

                let connect_target = 'find: {
                    /* try to connect to leader unless we failed within the last 5m */
                    if let Some(target) = self.peers.get(&self.leader) {
                        if 300_000 < now_ms() - target.last_peer_failure_epoch {
                            tracing::info!("use leader, no recent errors");
                            break 'find self.leader;
                        }
                    } else {
                        tracing::info!("peer info missing for leader so use as default");
                        break 'find self.leader;
                    }

                    let best_option = self
                        .peers
                        .iter()
                        .filter(|(addr, _)| !self.local.eq(addr))
                        .min_by_key(|(a_addr, a)| {
                            opt_count += 1;

                            if let Some(last_attempt) = &self.last_connect_attempt {
                                if last_attempt.eq(a_addr) {
                                    return (u64::MAX, 0);
                                }
                            }

                            let now = now_ms();
                            let mut since_fail = (now.max(a.last_peer_failure_epoch)
                                - a.last_peer_failure_epoch)
                                .max(300_000);

                            /* if leader, make fail distance 2x to give priority */
                            if self.leader.eq(a_addr) {
                                since_fail <<= 1;
                            }

                            let since_fail_key = if 60_000 < since_fail {
                                (since_fail >> 14) << 14
                            } else {
                                (since_fail >> 12) << 12
                            };

                            /* round by ~10ms */
                            let latency =
                                a.latency.as_ref().map(|v| v.latency_ms).unwrap_or(300) >> 3;

                            (since_fail_key, latency)
                        });

                    match best_option {
                        Some((addr, _)) => *addr,
                        None => self.leader,
                    }
                };

                tracing::info!(
                    option_count = opt_count,
                    peers = self.peers.len(),
                    target = ?connect_target,
                    leader = ?self.leader,
                    last_connect_attempt = ?self.last_connect_attempt,
                    "found connect target"
                );

                let local_id = self.local;
                let io = self.io.clone();
                let conn_tx = self.conn_tx.clone();
                let conn_timeout = self.connect_timeout;
                let net_settings = self.net_settings.clone();
                let is_sentinel = self.settings_is_sentinel;

                self.last_connect_attempt = Some(connect_target.clone());
                self.waiting_for_connection_update = true;

                tokio::spawn(
                    async move {
                        let cancel = CancellationToken::new();

                        let conn_update = 'connect: {
                            let conn_res =
                                tokio::time::timeout(conn_timeout, io.connect(&connect_target))
                                    .await;

                            let conn = match conn_res {
                                Ok(Ok(conn)) => {
                                    tracing::info!("connected to target");
                                    conn
                                }
                                Err(_) => {
                                    tracing::error!("timeout connecting to peer");
                                    break 'connect ConnectionUpdate::ConnectFailed(connect_target);
                                }
                                Ok(Err(error)) => {
                                    tracing::error!(?error, "io error connecting to peer");
                                    break 'connect ConnectionUpdate::ConnectFailed(connect_target);
                                }
                            };

                            let mut from_server = {
                                let (from_server_tx, from_server_rx) =
                                    channel::<SyncResponse<I, D>>(1024);

                                tokio::spawn(
                                    cancel.clone().run_until_cancelled_owned(
                                        ReadChannel::<I, _> {
                                            input: conn.read,
                                            output: from_server_tx,
                                            settings: net_settings.clone(),
                                        }
                                        .start()
                                        .instrument(
                                            tracing::info_span!("ReadPeer", remote = ?conn.remote),
                                        ),
                                    ),
                                );

                                from_server_rx
                            };

                            let to_server = {
                                let (to_server_tx, to_server_rx) =
                                    channel::<SyncRequest<I, D>>(1024);

                                tokio::spawn(
                                    cancel.clone().run_until_cancelled_owned(
                                        WriteChannel::<I, _> {
                                            input: to_server_rx,
                                            output: conn.write,
                                            settings: net_settings,
                                        }
                                        .start()
                                        .instrument(
                                            tracing::info_span!("WritePeer", remote = ?conn.remote),
                                        ),
                                    ),
                                );

                                to_server_tx
                            };

                            if to_server
                                .send(SyncRequest::MyAddress(local_id))
                                .await
                                .is_err()
                            {
                                tracing::error!(
                                    "failed to send initial MyAddress message to upstream"
                                );
                            }
                            if to_server
                                .send(SyncRequest::NodeInfo { is_sentinel })
                                .await
                                .is_err()
                            {
                                tracing::error!("failed to send node info to upstream");
                            }

                            {
                                let now = now_ms();
                                if to_server
                                    .send(SyncRequest::Ping(now))
                                    .await
                                    .err_log("new send channel is closed")
                                    .is_err()
                                {
                                    break 'connect ConnectionUpdate::ConnectFailed(connect_target);
                                }

                                tracing::info!("send ping to peer, waiting for pong");

                                let latency = loop {
                                    let pong_res = tokio::time::timeout(
                                        Duration::from_secs(5),
                                        from_server.recv(),
                                    )
                                    .await;
                                    match pong_res {
                                        Err(_) => {
                                            tracing::error!("timeout waiting for pong from peer");
                                            break 'connect ConnectionUpdate::ConnectFailed(
                                                connect_target,
                                            );
                                        }
                                        Ok(None) => {
                                            tracing::error!("channel closed waiting for pong");
                                            break 'connect ConnectionUpdate::ConnectFailed(
                                                connect_target,
                                            );
                                        }
                                        Ok(Some(SyncResponse::Pong(pong))) if pong == now => {
                                            break now_ms() - pong;
                                        }
                                        Ok(Some(SyncResponse::NodeInfo { .. }))
                                        | Ok(Some(SyncResponse::PeerInfo(_))) => continue,
                                        Ok(Some(_)) => {
                                            tracing::error!("got unexpected ping response");
                                            break 'connect ConnectionUpdate::ConnectFailed(
                                                connect_target,
                                            );
                                        }
                                    }
                                };

                                tracing::info!(latency, "got pong from peer");
                            }

                            let leader_path = {
                                if to_server
                                    .send(SyncRequest::ShareLeaderPath)
                                    .await
                                    .err_log("new send channel is closed")
                                    .is_err()
                                {
                                    break 'connect ConnectionUpdate::ConnectFailed(connect_target);
                                }

                                tracing::info!("request leader path from peer");

                                loop {
                                    let peer_res = tokio::time::timeout(
                                        Duration::from_secs(5),
                                        from_server.recv(),
                                    )
                                    .await;
                                    match peer_res {
                                        Err(_) => {
                                            tracing::error!("timeout waiting for path to leader");
                                            break 'connect ConnectionUpdate::ConnectFailed(
                                                connect_target,
                                            );
                                        }
                                        Ok(None) => {
                                            tracing::error!(
                                                "channel closed waiting for leader path"
                                            );
                                            break 'connect ConnectionUpdate::ConnectFailed(
                                                connect_target,
                                            );
                                        }
                                        Ok(Some(SyncResponse::LeaderPath(path))) => break path,
                                        Ok(Some(SyncResponse::NodeInfo { .. }))
                                        | Ok(Some(SyncResponse::PeerInfo(_))) => continue,
                                        Ok(Some(_)) => {
                                            tracing::error!("got unexpected leader path response");
                                            break 'connect ConnectionUpdate::ConnectFailed(
                                                connect_target,
                                            );
                                        }
                                    }
                                }
                            };

                            let _ = to_server.try_send(SyncRequest::SendMePeers);
                            let _ = to_server.try_send(SyncRequest::SendMePeerInfo);

                            ConnectionUpdate::UpdatedFollow(FollowState {
                                remote: conn.remote,
                                leader_path,
                                to_leader: to_server,
                                from_leader: from_server,
                                cancel: cancel.clone(),
                                feed_updater: None,
                            })
                        };

                        if matches!(conn_update, ConnectionUpdate::ConnectFailed(_)) {
                            cancel.cancel();
                        }

                        let _ = conn_tx
                            .send(conn_update)
                            .await
                            .err_log("failed to queue connection update");
                    }
                    .instrument(tracing::info_span!("connect", target = ?connect_target)),
                );
            }
            Event::ConnectionUpdate(update) => match update {
                ConnectionUpdate::ConnectFailed(address) => {
                    match self.peers.entry(address) {
                        hash_map::Entry::Vacant(v) => {
                            v.insert(DiscoveredPeer {
                                latency: None,
                                last_peer_failure_epoch: now_ms(),
                                last_client_msg_epoch: 0,
                                client_send: None,
                                is_sentinel: false,
                            });
                        }
                        hash_map::Entry::Occupied(o) => {
                            o.into_mut().last_peer_failure_epoch = now_ms();
                        }
                    }

                    self.timers
                        .get(Timer::ConnectToPeer)
                        .set_earlier(Duration::from_secs(1));
                }
                ConnectionUpdate::SentinelConnected {
                    address,
                    send,
                    cancel,
                } => {
                    self.mark_peer_sentinel(address, true);
                    self.sentinel_stats
                        .entry(address)
                        .or_default()
                        .mark_connected();

                    if let Some(existing) = self
                        .sentinel_links
                        .insert(address, SentinelLink { cancel, send })
                    {
                        existing.cancel.cancel();
                    }
                }
                ConnectionUpdate::SentinelDisconnected(address) => {
                    if let Some(existing) = self.sentinel_links.remove(&address) {
                        existing.cancel.cancel();
                    }
                    self.sentinel_stats
                        .entry(address)
                        .or_default()
                        .mark_disconnected();
                    self.timers
                        .get(Timer::ConnectSentinels)
                        .set_earlier(Duration::from_secs(1));
                }
                ConnectionUpdate::UpdatedFollow(follow) => {
                    if let Some(existing) = self.follow_state.replace(follow) {
                        existing.cancel.cancel();
                    }

                    let state = self
                        .follow_state
                        .as_ref()
                        .panic("just set follow, should not be missing");

                    self.timers.get(Timer::ConnectToPeer).clear();

                    if self.leader == self.local {
                        let removed = self.follow_state.take().unwrap();
                        removed.cancel.cancel();
                        assert!(self.updater.is_leading());
                        return;
                    }

                    if state.leader_path.is_empty()
                        || state.leader_path[0] != self.leader
                        || state.leader_path.contains(&self.local)
                    {
                        tracing::error!(
                            path = ?state.leader_path,
                            peer = ?state.remote,
                            leader = ?self.leader,
                            local = ?self.local,
                            "peer we're following has invalid leader path"
                        );
                        self.timers.get(Timer::RejectPeer).now();
                        return;
                    }

                    let details = self.updater.go_offline().await.recovery_details();

                    if state
                        .to_leader
                        .try_send(SyncRequest::SubscribeRecovery(details))
                        .is_err()
                    {
                        tracing::error!("failed to send recovery request to peer");
                        self.timers.get(Timer::RejectPeer).now();
                        return;
                    }

                    self.timers
                        .get(Timer::SendVerifyLeader)
                        .set(Duration::from_secs(10));
                    self.timers
                        .get(Timer::RequireVerifyLeader)
                        .set(Duration::from_secs(30));
                    self.timers
                        .get(Timer::RequireState)
                        .set(self.receive_state_timeout);
                }
            },
            Event::LeaderMessage(msg) => {
                let follow = self.follow_state.as_mut().panic("follow_state missing");

                match msg {
                    SyncResponse::NodeInfo { is_sentinel } => {
                        let remote = follow.remote;
                        self.mark_peer_sentinel(remote, is_sentinel);
                    }
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
                                    last_peer_failure_epoch: 0,
                                    last_client_msg_epoch: 0,
                                    client_send: None,
                                    is_sentinel: false,
                                });
                            }
                            hash_map::Entry::Occupied(o) => {
                                o.into_mut().latency.replace(latency);
                            }
                        }
                    }
                    SyncResponse::FreshState(state) => {
                        if follow.feed_updater.is_some() {
                            tracing::error!("already in session with leader but got fresh state");
                            self.timers.get(Timer::RejectPeer).now();
                            return;
                        }

                        let (action_tx, mut action_rx) = channel(1024);
                        let (authority_tx, authority_rx) = channel(1024);
                        let seq = state.accept_seq();

                        self.updater
                            .follow(
                                action_tx,
                                NewFollowState {
                                    state,
                                    authority_rx: SequencedReceiver::new(seq, authority_rx),
                                }
                                .try_into_valid()
                                .panic("could not create valid follow state"),
                            )
                            .await;

                        follow.feed_updater = Some(SequencedSender::new(seq, authority_tx));

                        /* forward actions to leader */
                        {
                            let to_leader = follow.to_leader.clone();
                            tokio::spawn(follow.cancel.clone().run_until_cancelled_owned(
                                async move {
                                    while let Some((source, action)) = action_rx.recv().await {
                                        tokio::task::yield_now().await;
                                        if to_leader
                                            .send(SyncRequest::Action { source, action })
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        }
                                    }

                                    tokio::task::yield_now().await;
                                    tracing::error!(
                                        "forwarding action to leader task stopped without cancel"
                                    );
                                },
                            ));
                        }

                        tracing::info!("Connected to leader with FreshState");
                        self.timers.get(Timer::RequireState).clear();
                        self.last_connect_attempt = None;
                    }
                    SyncResponse::RecoveryAccepted(seq) => {
                        if follow.feed_updater.is_some() {
                            tracing::error!("already in session with leader but got fresh state");
                            self.timers.get(Timer::RejectPeer).now();
                            return;
                        }

                        let (action_tx, mut action_rx) = channel(1024);
                        let (authority_tx, authority_rx) = channel(1024);

                        let success = self
                            .updater
                            .try_follow(
                                action_tx,
                                FollowTarget {
                                    leader_state_check: None,
                                    authority_rx: SequencedReceiver::new(seq, authority_rx),
                                },
                            )
                            .await;

                        if !success {
                            tracing::error!("failed to recover connection from leader");
                            self.timers.get(Timer::RejectPeer).now();
                            return;
                        }

                        follow.feed_updater = Some(SequencedSender::new(seq, authority_tx));

                        /* forward actions to leader */
                        {
                            let to_leader = follow.to_leader.clone();
                            tokio::spawn(follow.cancel.clone().run_until_cancelled_owned(
                                async move {
                                    while let Some((source, action)) = action_rx.recv().await {
                                        tokio::task::yield_now().await;
                                        if to_leader
                                            .send(SyncRequest::Action { source, action })
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        }
                                    }

                                    tokio::task::yield_now().await;
                                    tracing::error!(
                                        "forwarding action to leader task stopped without cancel"
                                    );
                                },
                            ));
                        }

                        tracing::info!("Connected to leader with Recovery");
                        self.timers.get(Timer::RequireState).clear();
                        self.last_connect_attempt = None;
                    }
                    SyncResponse::AuthorityAction(seq, action) => {
                        let Some(updater) = follow.feed_updater.as_mut() else {
                            tracing::error!("got AuthorityAction but not following leader yet");
                            self.timers.get(Timer::RejectPeer).now();
                            return;
                        };

                        if let Err(error) = updater.safe_send(seq, action).await {
                            tracing::error!(?error, "leader sent invalid sequence");
                            self.timers.get(Timer::RejectPeer).now();
                        }
                    }
                    SyncResponse::LeaderPath(path) => {
                        let is_valid = 'check: {
                            if path.is_empty() {
                                tracing::error!("got empty leader path from peer");
                                break 'check false;
                            }

                            if path[0] != self.leader {
                                tracing::debug!("leader path doesn't go to same leader");
                                break 'check false;
                            }

                            for step in &path[1..] {
                                if *step == self.local {
                                    tracing::warn!("leader path from peer includes us");
                                    self.timers.get(Timer::RejectPeer).now();
                                    return;
                                }
                            }

                            true
                        };

                        if is_valid {
                            follow.leader_path = path;

                            self.timers
                                .get(Timer::SendVerifyLeader)
                                .set_earlier(Duration::from_secs(10));
                            self.timers
                                .get(Timer::RequireVerifyLeader)
                                .set(Duration::from_secs(40));
                        } else {
                            follow.leader_path = vec![];

                            self.timers
                                .get(Timer::SendVerifyLeader)
                                .set_earlier(Duration::from_secs(2));
                            self.timers
                                .get(Timer::RequireVerifyLeader)
                                .extend(Duration::from_secs(4), Duration::from_secs(20))
                                .set_earlier(Duration::from_secs(2) + self.connect_timeout);
                        }
                    }
                    SyncResponse::Peers(peers) => {
                        for peer in peers {
                            if let hash_map::Entry::Vacant(v) = self.peers.entry(peer) {
                                tracing::info!("Informed of new new peer: {:?}", peer);
                                v.insert(DiscoveredPeer {
                                    latency: None,
                                    last_peer_failure_epoch: 0,
                                    last_client_msg_epoch: 0,
                                    client_send: None,
                                    is_sentinel: false,
                                });
                            }
                        }
                    }
                    SyncResponse::PeerInfo(peers) => {
                        self.apply_peer_info(peers);
                    }
                }
            }
            Event::Control(ControlMessage::SetLeader(leader)) => {
                self.set_leader_inner(leader).await;
            }
            Event::ClientMessage(ClientMessage { client, msg, send }) => {
                if client.cancel.is_cancelled() {
                    return;
                }

                match self.peers.entry(client.source_id) {
                    hash_map::Entry::Vacant(v) => {
                        tracing::info!(client_id = ?client.source_id, "Discovered Peer");
                        self.timers
                            .get(Timer::BroadcastPeers)
                            .extend(Duration::from_secs(1), Duration::from_secs(5));

                        v.insert(DiscoveredPeer {
                            latency: None,
                            last_client_msg_epoch: now_ms(),
                            last_peer_failure_epoch: 0,
                            client_send: Some(send.clone()),
                            is_sentinel: false,
                        });
                    }
                    hash_map::Entry::Occupied(o) => {
                        let peer = o.into_mut();
                        peer.last_client_msg_epoch = now_ms();

                        if peer
                            .client_send
                            .as_ref()
                            .map(|v| send.same_channel(v))
                            .unwrap_or(true)
                        {
                            peer.client_send = Some(send.clone());
                        }
                    }
                }

                match msg {
                    SyncRequest::MyAddress(addr) => {
                        if addr != client.source_id {
                            tracing::warn!("got MyAddress which doesn't match source id");
                        }
                    }
                    SyncRequest::NodeInfo { is_sentinel } => {
                        self.mark_peer_sentinel(client.source_id, is_sentinel);
                        let _ = send.try_send(SyncResponse::NodeInfo {
                            is_sentinel: self.settings_is_sentinel,
                        });
                    }
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

                        let peers = self.peers.keys().cloned().collect::<Vec<_>>();
                        permit.send(SyncResponse::Peers(peers));
                    }
                    SyncRequest::SendMePeerInfo => {
                        self.metrics.client_req_send_me_peers.inc();

                        let Ok(permit) = send.try_reserve() else {
                            self.metrics.client_send_dropped.inc();
                            return;
                        };

                        permit.send(SyncResponse::PeerInfo(self.peer_infos()));
                    }
                    SyncRequest::NoticePeers(peers) => {
                        self.metrics.client_req_notice_peers.inc();

                        for peer in peers {
                            if let hash_map::Entry::Vacant(v) = self.peers.entry(peer) {
                                v.insert(DiscoveredPeer {
                                    latency: None,
                                    last_peer_failure_epoch: 0,
                                    last_client_msg_epoch: 0,
                                    client_send: None,
                                    is_sentinel: false,
                                });
                            }
                        }
                    }
                    SyncRequest::NoticePeerInfo(peers) => {
                        self.metrics.client_req_notice_peers.inc();
                        self.apply_peer_info(peers);
                    }
                    SyncRequest::SentinelConnectivityReport(report) => {
                        if self.settings_is_sentinel {
                            self.peer_sentinel_reports.insert(
                                client.source_id,
                                TimedPeerSentinelReport {
                                    reports: report,
                                    epoch: now_ms(),
                                },
                            );
                        }
                    }
                    SyncRequest::SentinelVote(vote) => {
                        if self.settings_is_sentinel {
                            self.mark_peer_sentinel(client.source_id, true);
                            self.sentinel_votes.insert(
                                client.source_id,
                                TimedSentinelVote {
                                    vote,
                                    epoch: now_ms(),
                                },
                            );
                            self.timers
                                .get(Timer::EvaluateSentinelElection)
                                .set_earlier(Duration::from_millis(10));
                        }
                    }
                    SyncRequest::ShareLeaderPath => {
                        self.metrics.client_req_whois_leader.inc();

                        let leader_path = if self.local == self.leader {
                            vec![self.local]
                        } else if self.updater.is_offline() {
                            vec![]
                        } else if let Some(state) = &self.follow_state {
                            let mut path = state.leader_path.clone();
                            path.push(self.local);
                            path
                        } else {
                            vec![]
                        };

                        if send
                            .try_send(SyncResponse::LeaderPath(leader_path))
                            .is_err()
                        {
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

                        let Some(mut follow_target) = self.updater.add_subscriber(recovery).await
                        else {
                            self.metrics.client_recovery_fails.inc();

                            if self
                                .client_msg_tx
                                .try_send(ClientMessage {
                                    client: client.clone(),
                                    msg: SyncRequest::SubscribeFresh,
                                    send,
                                })
                                .is_err()
                            {
                                self.metrics.event_queue_fail.inc();
                                client.cancel.cancel();
                            }

                            return;
                        };

                        let exclusive = client
                            .mode
                            .compare_exchange(
                                MODE_CONNECTING,
                                MODE_CONNECTED,
                                std::sync::atomic::Ordering::SeqCst,
                                std::sync::atomic::Ordering::SeqCst,
                            )
                            .is_ok();

                        if !exclusive {
                            self.metrics.client_dual_connect_error.inc();
                            return;
                        }

                        tokio::spawn(client.cancel.clone().run_until_cancelled_owned(async move {
                            if send
                                .send(SyncResponse::RecoveryAccepted(
                                    follow_target.authority_rx.next_seq(),
                                ))
                                .await
                                .is_err()
                            {
                                client.cancel.cancel();
                                return;
                            }

                            while let Some(msg) = follow_target.authority_rx.recv().await {
                                tokio::task::yield_now().await;
                                if send
                                    .send(SyncResponse::AuthorityAction(msg.0, msg.1))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }

                            client.cancel.cancel();
                        }));
                    }
                    SyncRequest::SubscribeFresh => {
                        self.metrics.client_req_fresh.inc();

                        let exclusive = client
                            .mode
                            .compare_exchange(
                                MODE_CONNECTING,
                                MODE_CONNECTED,
                                std::sync::atomic::Ordering::SeqCst,
                                std::sync::atomic::Ordering::SeqCst,
                            )
                            .is_ok();

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
                                tokio::task::yield_now().await;
                                if send
                                    .send(SyncResponse::AuthorityAction(msg.0, msg.1))
                                    .await
                                    .is_err()
                                {
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
}

struct ClientAcceptor<I: SyncIO, D: DeterministicState> {
    io: Arc<I>,
    msg_tx: Sender<ClientMessage<I, D>>,
    net_settings: NetIoSettings,
    metrics: Arc<SyncManagerMetrics>,
    is_sentinel: bool,
}

impl<I: SyncIO, D: DeterministicState> ClientAcceptor<I, D>
where
    D: MessageEncoding,
    D::AuthorityAction: MessageEncoding + Clone,
    D::Action: MessageEncoding,
{
    pub async fn start(self) {
        loop {
            tokio::task::yield_now().await;

            let client = tokio::select! {
                _ = self.msg_tx.closed() => {
                    tracing::info!("client message receiver closed, stopping client acceptor");
                    break;
                }
                client_res = self.io.next_client() => {
                    match client_res {
                        Ok(res) => res,
                        Err(error) => {
                            tracing::error!(?error, "got error accepting next client");
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                    }
                }
            };

            tracing::info!(remote = ?client.remote, "got new client");
            self.add_client(client);
        }
    }

    pub fn add_client(&self, client: SyncConnection<I>) {
        let event_tx = self.msg_tx.clone();
        let metrics = self.metrics.clone();
        let net_settings = self.net_settings.clone();
        let is_sentinel = self.is_sentinel;

        tokio::spawn(async move {
            let cancel = CancellationToken::new();

            let mut from_client = {
                let (from_client_tx, from_client_rx) = channel::<SyncRequest<I, D>>(1024);

                tokio::spawn(
                    cancel.clone().run_until_cancelled_owned(
                        ReadChannel::<I, _> {
                            input: client.read,
                            output: from_client_tx,
                            settings: net_settings.clone(),
                        }
                        .start()
                        .instrument(tracing::info_span!("ReadClient", remote = ?client.remote)),
                    ),
                );

                from_client_rx
            };

            let addr_res = tokio::time::timeout(Duration::from_secs(1), async {
                'get_addr: {
                    while let Some(msg) = from_client.recv().await {
                        let SyncRequest::MyAddress(addr) = msg else {
                            continue;
                        };
                        break 'get_addr Some(addr);
                    }

                    None
                }
            })
            .await;

            let Ok(Some(addr)) = addr_res else {
                tracing::error!("failed to get address from peer");
                cancel.cancel();
                return;
            };

            let state = Arc::new(ClientState {
                source_id: addr,
                mode: AtomicU64::new(MODE_CONNECTING),
                cancel,
            });

            let to_client = {
                let (to_client_tx, to_client_rx) = channel::<SyncResponse<I, D>>(1024);

                tokio::spawn(
                    state.cancel.clone().run_until_cancelled_owned(
                        WriteChannel::<I, _> {
                            input: to_client_rx,
                            output: client.write,
                            settings: net_settings,
                        }
                        .start()
                        .instrument(tracing::info_span!("WriteClient", remote = ?client.remote)),
                    ),
                );

                to_client_tx
            };

            let _ = to_client.try_send(SyncResponse::NodeInfo { is_sentinel });

            tokio::spawn(async move {
                metrics.clients_connected.inc();

                state
                    .cancel
                    .clone()
                    .run_until_cancelled_owned(async move {
                        while let Some(msg) = from_client.recv().await {
                            tokio::task::yield_now().await;
                            if event_tx
                                .send(ClientMessage {
                                    client: state.clone(),
                                    msg,
                                    send: to_client.clone(),
                                })
                                .await
                                .is_err()
                            {
                                tracing::warn!(
                                    "SyncManager worker closed, stopping read from client"
                                );
                                break;
                            }
                        }

                        state.cancel.cancel();
                    })
                    .await;

                metrics.clients_connected.dec();
            });
        });
    }
}

#[derive(Debug, Default)]
struct TimerOpt {
    expires_at_start: Option<Instant>,
    expires_at: Option<Instant>,
    trigger: bool,
}

impl TimerOpt {
    fn is_set(&self) -> bool {
        self.trigger || self.expires_at.is_some()
    }

    fn now(&mut self) {
        self.clear();
        self.trigger = true;
    }

    fn extend(&mut self, wait: Duration, max: Duration) -> &mut Self {
        let now = Instant::now();
        let mut expires_at = now + wait;

        if let Some(start) = self.expires_at_start {
            let limit = start + max;
            if let Some(current) = self.expires_at {
                expires_at = expires_at.max(current);
            }

            self.expires_at = Some(expires_at.min(limit));
        } else {
            let limit = now + max;
            if let Some(current) = self.expires_at {
                self.expires_at = Some(expires_at.max(current).min(limit));
            } else {
                self.expires_at = Some(expires_at.min(limit));
            }
            self.expires_at_start = Some(now);
        }

        self
    }

    fn set(&mut self, wait: Duration) {
        self.trigger = false;
        let now = Instant::now();
        self.expires_at = Some(now + wait);
        self.expires_at_start = Some(now);
    }

    fn set_earlier(&mut self, wait: Duration) {
        let now = Instant::now();
        let updated = now + wait;

        if self.expires_at.is_none() {
            self.expires_at_start = Some(now);
        }

        self.expires_at = match self.expires_at {
            Some(opt) if opt < updated => Some(opt),
            _ => Some(updated),
        };
    }

    fn clear(&mut self) {
        self.trigger = false;
        self.expires_at.take();
        self.expires_at_start.take();
    }
}

pub const MODE_CONNECTING: u64 = 0;
pub const MODE_CONNECTED: u64 = 1;

pub struct ClientState<S: SourceId> {
    source_id: S,
    mode: AtomicU64,
    cancel: CancellationToken,
}

struct DiscoveredPeer<I: SyncIO, D: DeterministicState> {
    latency: Option<Latency>,
    last_peer_failure_epoch: u64,
    last_client_msg_epoch: u64,
    client_send: Option<Sender<SyncResponse<I, D>>>,
    is_sentinel: bool,
}

impl<I: SyncIO, D: DeterministicState> DiscoveredPeer<I, D> {
    fn new() -> Self {
        Self {
            latency: None,
            last_peer_failure_epoch: 0,
            last_client_msg_epoch: 0,
            client_send: None,
            is_sentinel: false,
        }
    }
}

struct Latency {
    last_pong_epoch: u64,
    latency_ms: u64,
}

#[cfg(test)]
mod test {
    use crate::testing::{
        setup_logging,
        state_tests::{TestState, TestStateAction},
        test_sync_io::{TestIOKillMode, TestSyncNet},
    };

    use super::*;

    fn sentinel_test_settings(initial_peers: Vec<u64>) -> SyncMangerSettings<u64> {
        SyncMangerSettings {
            initial_peers,
            is_sentinel: true,
            connect_timeout: Duration::from_millis(200),
            receive_state_timeout: Duration::from_secs(2),
            sentinel: SentinelSettings {
                report_interval: Duration::from_millis(100),
                election_interval: Duration::from_millis(100),
                unhealthy_disconnect_threshold: 1,
                stale_report_after: Duration::from_secs(2),
            },
            ..Default::default()
        }
    }

    async fn wait_for<F: FnMut() -> bool>(mut check: F, timeout: Duration) {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(check());
    }

    #[tokio::test]
    async fn sync_manager_sentinel_majority_promotes_new_master_test() {
        setup_logging();

        let test_net = TestSyncNet::new();

        let a = test_net.io(1).await;
        let b = test_net.io(2).await;
        let c = test_net.io(3).await;

        let a_work = {
            let _span = tracing::info_span!("A").entered();
            SyncManager::new(
                a,
                1,
                TestState::default(),
                sentinel_test_settings(vec![2, 3]),
            )
        };
        let b_work = {
            let _span = tracing::info_span!("B").entered();
            SyncManager::new(
                b,
                2,
                TestState::default(),
                sentinel_test_settings(vec![1, 3]),
            )
        };
        let c_work = {
            let _span = tracing::info_span!("C").entered();
            SyncManager::new(
                c,
                3,
                TestState::default(),
                sentinel_test_settings(vec![1, 2]),
            )
        };

        b_work.set_leader(1).await;
        c_work.set_leader(1).await;

        a_work
            .action_tx()
            .send(TestStateAction::Set { slot: 0, value: 10 })
            .await
            .unwrap();

        wait_for(
            || {
                b_work.shared().read().state().numbers[0] == 10
                    && c_work.shared().read().state().numbers[0] == 10
            },
            Duration::from_secs(4),
        )
        .await;

        let old_id = b_work.shared().read().details().id;

        test_net.block_connection(1, 2, true).await;
        test_net.block_connection(1, 3, true).await;
        test_net
            .kill_connection(1, 2, TestIOKillMode::Shutdown)
            .await;
        test_net
            .kill_connection(1, 3, TestIOKillMode::Shutdown)
            .await;

        wait_for(
            || {
                let b_shared = b_work.shared();
                let c_shared = c_work.shared();
                let b = b_shared.read();
                let c = c_shared.read();
                b.details().id != old_id && b.details().id == c.details().id
            },
            Duration::from_secs(8),
        )
        .await;

        c_work
            .action_tx()
            .send(TestStateAction::Add { slot: 0, value: 5 })
            .await
            .unwrap();

        wait_for(
            || {
                b_work.shared().read().state().numbers[0] == 15
                    && c_work.shared().read().state().numbers[0] == 15
            },
            Duration::from_secs(4),
        )
        .await;
    }

    #[tokio::test]
    async fn sync_manager_doesnt_allow_peer_with_different_leader_test() {
        setup_logging();

        let test_net = TestSyncNet::new();

        let a = test_net.io(1).await;
        let b = test_net.io(2).await;
        let c = test_net.io(3).await;

        let a_work = {
            let _span = tracing::info_span!("A").entered();
            SyncManager::new(
                a,
                1,
                TestState {
                    sequence: 99999,
                    ..Default::default()
                },
                Default::default(),
            )
        };
        let b_work = {
            let _span = tracing::info_span!("B").entered();
            SyncManager::new(b, 2, TestState::default(), Default::default())
        };
        let c_work = {
            let _span = tracing::info_span!("C").entered();
            SyncManager::new(c, 3, TestState::default(), Default::default())
        };

        b_work.set_leader(1).await;
        c_work.set_leader(2).await;

        let tx = a_work.action_tx();
        tx.send(TestStateAction::Set { slot: 0, value: 99 })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(a_work.shared().read().state().numbers[0], 99);
        assert_eq!(b_work.shared().read().state().numbers[0], 99);
        assert_eq!(c_work.shared().read().state().numbers[0], 0);

        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    #[tokio::test]
    async fn sync_manager_connects_through_peer_test() {
        setup_logging();

        let test_net = TestSyncNet::new();

        let a = test_net.io(1).await;
        let b = test_net.io(2).await;
        let c = test_net.io(3).await;

        let a_work = {
            let _span = tracing::info_span!("A").entered();
            SyncManager::new(
                a,
                1,
                TestState {
                    sequence: 99999,
                    ..Default::default()
                },
                Default::default(),
            )
        };
        let b_work = {
            let _span = tracing::info_span!("B").entered();
            SyncManager::new(b, 2, TestState::default(), Default::default())
        };
        let c_work = {
            let _span = tracing::info_span!("C").entered();
            SyncManager::new(c, 3, TestState::default(), Default::default())
        };

        b_work.set_leader(1).await;
        c_work.set_leader(1).await;

        // tokio::time::sleep(Duration::from_millis(200)).await;

        let tx = a_work.action_tx();
        tx.send(TestStateAction::Set { slot: 0, value: 99 })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(a_work.shared().read().state().numbers[0], 99);
        assert_eq!(b_work.shared().read().state().numbers[0], 99);
        assert_eq!(c_work.shared().read().state().numbers[0], 99);

        tokio::time::sleep(Duration::from_secs(1)).await;

        test_net.block_connection(1, 2, true).await;
        test_net
            .kill_connection(1, 2, TestIOKillMode::Shutdown)
            .await;

        tx.send(TestStateAction::Add {
            slot: 0,
            value: 100,
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(a_work.shared().read().state().numbers[0], 199);
        assert_eq!(b_work.shared().read().state().numbers[0], 199);
        assert_eq!(c_work.shared().read().state().numbers[0], 199);

        println!("DONE");
    }

    #[tokio::test]
    async fn sync_manager_uses_initial_peers_before_discovery_test() {
        setup_logging();

        let test_net = TestSyncNet::new();

        let a = test_net.io(1).await;
        let b = test_net.io(2).await;
        let c = test_net.io(3).await;

        let a_work = {
            let _span = tracing::info_span!("A").entered();
            SyncManager::new(
                a,
                1,
                TestState {
                    sequence: 99999,
                    ..Default::default()
                },
                Default::default(),
            )
        };
        let c_work = {
            let _span = tracing::info_span!("C").entered();
            SyncManager::new(c, 3, TestState::default(), Default::default())
        };

        c_work.set_leader(1).await;

        let tx = a_work.action_tx();
        tx.send(TestStateAction::Set { slot: 0, value: 99 })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(c_work.shared().read().state().numbers[0], 99);

        test_net.block_connection(1, 2, true).await;

        let b_work = {
            let _span = tracing::info_span!("B").entered();
            SyncManager::new(
                b,
                2,
                TestState::default(),
                SyncMangerSettings {
                    initial_peers: vec![3],
                    ..Default::default()
                },
            )
        };

        b_work.set_leader(1).await;

        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(b_work.shared().read().state().numbers[0], 99);

        tx.send(TestStateAction::Add {
            slot: 0,
            value: 100,
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(a_work.shared().read().state().numbers[0], 199);
        assert_eq!(b_work.shared().read().state().numbers[0], 199);
        assert_eq!(c_work.shared().read().state().numbers[0], 199);
    }

    #[tokio::test]
    async fn sync_manager_initial_peers_ignore_local_and_dedup_test() {
        setup_logging();

        let test_net = TestSyncNet::new();
        let a = test_net.io(1).await;

        let worker = SyncManagerWorker::<_, TestState>::new(
            a,
            1,
            channel(1).1,
            TestState::default(),
            SyncMangerSettings {
                initial_peers: vec![1, 2, 2, 3, 3],
                ..Default::default()
            },
        );

        assert_eq!(worker.peers.len(), 2);
        assert!(!worker.peers.contains_key(&1));
        assert!(worker.peers.contains_key(&2));
        assert!(worker.peers.contains_key(&3));
    }
}
