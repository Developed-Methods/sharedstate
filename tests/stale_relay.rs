//! A follower that cannot reach the leader relays through a peer that
//! follows the same leader. If that relay stops working without closing
//! the connection, the follower must notice and move on.
//!
//! The relay here passes the relay check (it follows the same leader),
//! accepts the subscription at the follower's position, and from then on
//! sends nothing but keepalives. RPCs to it are accepted and never
//! answered, so actions the follower forwards to it are lost. The
//! subscription sees no transport error and no leader change, so only the
//! sync task's own checks can end it.
//!
//! Nodes 1 and 2 are voters, node 4 is the follower. Node 3 is the stale
//! relay, played by a scripted peer. Cutting node 4 off from the voters
//! forces it onto the relay; healing the network must bring it back.

use std::{
    collections::BTreeMap,
    io,
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use message_encoding::MessageEncoding;
use sharedstate::{
    SharedState, SharedStateConfig, SharedStateSettings,
    cluster::{
        leader::{LeaderMode, LeaderTiming},
        node_state::SyncStatus,
        peer_discovery::PeerDiscoveryTiming,
        state_sync::StateSyncTiming,
    },
    protocol::messages::{LeaderState, SharePeerDetails, SyncRequest, SyncResponse},
    state::deterministic_state::DeterministicState,
    transport::{
        channels::NetIoSettings,
        simulated::{SimulatedIo, SimulatedNet},
        traits::{SyncConnection, SyncIOListener},
    },
};

const SYNC_SERVERS: [u64; 2] = [1, 2];
const LEADER: u64 = 1;
const STALE_RELAY: u64 = 3;
const FOLLOWER: u64 = 4;

/// How long the follower gets to catch up once the leader is reachable
/// again.
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(40);

/// How often the follower submits its periodic heartbeat action.
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(200);

type Node = SharedState<SimulatedIo, KvState>;

#[derive(Clone, Debug, Default)]
struct KvState {
    seq: u64,
    values: BTreeMap<u64, u64>,
}

impl DeterministicState for KvState {
    type Action = (u64, u64);
    type AuthorityAction = (u64, u64);

    fn accept_seq(&self) -> u64 {
        self.seq
    }

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
        action
    }

    fn update(&mut self, (key, value): &Self::AuthorityAction) {
        self.values.insert(*key, *value);
        self.seq += 1;
    }
}

impl MessageEncoding for KvState {
    fn write_to<T: io::Write>(&self, out: &mut T) -> io::Result<usize> {
        let mut sum = self.seq.write_to(out)?;
        sum += (self.values.len() as u64).write_to(out)?;
        for (key, value) in &self.values {
            sum += key.write_to(out)?;
            sum += value.write_to(out)?;
        }
        Ok(sum)
    }

    fn read_from<T: io::Read>(read: &mut T) -> io::Result<Self> {
        let seq = MessageEncoding::read_from(read)?;
        let len = u64::read_from(read)? as usize;
        let mut values = BTreeMap::new();
        for _ in 0..len {
            values.insert(MessageEncoding::read_from(read)?, MessageEncoding::read_from(read)?);
        }
        Ok(Self { seq, values })
    }
}

fn net_settings() -> NetIoSettings {
    NetIoSettings {
        process_timeout: Duration::from_millis(500),
        message_timeout: Duration::from_secs(1),
    }
}

/// Short network and discovery timings; the sync task keeps its defaults.
fn fast_settings() -> SharedStateSettings {
    SharedStateSettings {
        net: net_settings(),
        broadcast: Default::default(),
        discovery_timing: PeerDiscoveryTiming {
            observation_interval: Duration::from_millis(50),
            max_concurrent_observations: 8,
            gateway_view_ttl: Duration::from_secs(2),
        },
        leader_timing: LeaderTiming {
            tick_interval: Duration::from_millis(25),
        },
        sync_timing: StateSyncTiming {
            leader_poll_interval: Duration::from_millis(20),
            retry_delay: Duration::from_millis(50),
            ..Default::default()
        },
        peer_expiry: Default::default(),
    }
}

async fn start_node(net: &SimulatedNet, address: u64, can_lead: bool, peers: &[u64]) -> Node {
    SharedState::start(SharedStateConfig {
        io: net.start_io(address).await,
        my_address: address,
        can_lead,
        accessible: true,
        voter_gateway: None,
        initial_peers: peers.iter().copied().filter(|peer| *peer != address).collect(),
        initial_state: KvState::default(),
        settings: fast_settings(),
    })
    .unwrap()
}

/// A relay that stops working without closing its connections. Until
/// someone subscribes it behaves like a healthy observer fed by the
/// leader. The moment it accepts a subscription it freezes: the feed stays
/// open on keepalives and carries no actions, and every RPC, on old
/// connections or new ones, is read and never answered.
struct StaleRelay {
    frozen: AtomicBool,
    subscriptions: AtomicUsize,
    leader_state: LeaderState<u64>,
}

impl StaleRelay {
    async fn start(net: &SimulatedNet, leader_state: LeaderState<u64>) -> Arc<Self> {
        let relay = Arc::new(Self {
            frozen: AtomicBool::new(false),
            subscriptions: AtomicUsize::new(0),
            leader_state,
        });
        let io = net.start_io(STALE_RELAY).await;
        let accepting = relay.clone();
        tokio::spawn(async move {
            while let Ok(connection) = io.next_client().await {
                tokio::spawn(accepting.clone().serve(connection));
            }
        });
        relay
    }

    async fn serve(self: Arc<Self>, connection: SyncConnection<SimulatedIo>) {
        let (_remote, write, mut read) = connection.server_channels::<KvState>(net_settings());

        while let Some(request) = read.recv().await {
            if self.frozen.load(Ordering::SeqCst) {
                continue;
            }
            let response = match request {
                SyncRequest::ProtocolVersion(_)
                | SyncRequest::MyAddress(_)
                | SyncRequest::MyInaccessibleAddress(_)
                | SyncRequest::LeaderInformation(_)
                | SyncRequest::Action { .. } => SyncResponse::Ok,
                SyncRequest::SharePeers(_) => SyncResponse::Peers(vec![SharePeerDetails {
                    address: STALE_RELAY,
                    can_be_leader: Some(false),
                    last_global_activity: NonZeroU64::new(now_ms()),
                }]),
                SyncRequest::LeaderQuery => SyncResponse::LeaderState(self.leader_state.clone()),
                SyncRequest::SubscribeRecovery(details) => {
                    self.frozen.store(true, Ordering::SeqCst);
                    self.subscriptions.fetch_add(1, Ordering::SeqCst);
                    SyncResponse::Accepted(details.next_seq())
                }
                SyncRequest::SubscribeFresh | SyncRequest::LeaderInfoQuery => SyncResponse::UnexpectedRequest,
            };
            if write.send(response).await.is_err() {
                return;
            }
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

fn sees(node: &Node, key: u64, value: u64) -> bool {
    node.state_handle()
        .read_with(|state| state.state().values.get(&key) == Some(&value))
}

async fn wait_until(what: &str, timeout: Duration, mut check: impl AsyncFnMut() -> bool) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if check().await {
            return Ok(());
        }
        if deadline <= Instant::now() {
            return Err(format!("timed out after {timeout:?} waiting for {what}"));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn set_voter_links_blocked(net: &SimulatedNet, blocked: bool) {
    for sync_server in SYNC_SERVERS {
        net.set_edge_blocked(sync_server, FOLLOWER, blocked).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_leaves_stale_relay_once_leader_is_reachable() {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    let net = SimulatedNet::new();
    let all = [LEADER, 2, STALE_RELAY, FOLLOWER];
    let mut sync_servers = Vec::new();
    for address in SYNC_SERVERS {
        sync_servers.push(start_node(&net, address, true, &all).await);
    }
    let leader = &sync_servers[0];
    wait_until("node 1 to lead", Duration::from_secs(30), async || {
        matches!(leader.leader_state().await.mode, LeaderMode::Leading)
    })
    .await
    .unwrap();

    let following_leader = LeaderState {
        term: leader.leader_state().await.term,
        mode: LeaderMode::Following { leader: LEADER },
    };
    let relay = StaleRelay::start(&net, following_leader).await;
    let follower = start_node(&net, FOLLOWER, false, &all).await;

    /* healthy replication, with the relay known and connected so the
     * follower picks it first once it needs one */
    leader.submit_action((1, 1)).await.unwrap();
    wait_until("the follower to replicate key 1", Duration::from_secs(10), async || {
        sees(&follower, 1, 1)
    })
    .await
    .unwrap();
    wait_until("the follower to connect to the relay", Duration::from_secs(10), async || {
        let peers = follower.node().peers.lock().await;
        peers.get(&STALE_RELAY).is_some_and(|peer| peer.connect_status.is_connected())
    })
    .await
    .unwrap();

    /* the follower loses the voters and falls back to the relay */
    set_voter_links_blocked(&net, true).await;
    wait_until("the follower to subscribe through the relay", Duration::from_secs(30), async || {
        1 <= relay.subscriptions.load(Ordering::SeqCst)
            && matches!(*follower.node().sync_status.borrow(), SyncStatus::Relayed { relay: STALE_RELAY, .. })
    })
    .await
    .unwrap();

    /* the follower keeps submitting a heartbeat, dropping it when its
     * action queue is full rather than waiting */
    let heartbeats = Arc::new(AtomicU64::new(0));
    let heartbeat_task = {
        let actions = follower.actions_sender();
        let heartbeats = heartbeats.clone();
        tokio::spawn(async move {
            loop {
                let beat = heartbeats.fetch_add(1, Ordering::SeqCst) + 1;
                let _ = actions.try_send((FOLLOWER, (1000, beat)));
                tokio::time::sleep(HEARTBEAT_INTERVAL).await;
            }
        })
    };

    leader.submit_action((2, 2)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!sees(&follower, 2, 2), "the follower got leader updates while cut off from the voters");

    /* the network heals: the leader is reachable and would accept the
     * follower directly */
    set_voter_links_blocked(&net, false).await;
    let beat_at_heal = heartbeats.load(Ordering::SeqCst);
    leader.submit_action((3, 3)).await.unwrap();

    let caught_up = wait_until("the follower to see key 3", RECOVERY_TIMEOUT, async || sees(&follower, 3, 3)).await;
    let heartbeat_delivered = wait_until("a post-heal heartbeat to reach the leader", RECOVERY_TIMEOUT, async || {
        leader
            .state_handle()
            .read_with(|state| state.state().values.get(&1000).is_some_and(|beat| beat_at_heal < *beat))
    })
    .await;
    heartbeat_task.abort();

    let status = *follower.node().sync_status.borrow();
    let seq = follower.state_handle().read_with(|state| state.state().seq);
    assert!(
        caught_up.is_ok() && heartbeat_delivered.is_ok(),
        "follower stayed on the stale relay after the network healed \
         (sync status {status:?}, state seq {seq}):\n{caught_up:?}\n{heartbeat_delivered:?}",
    );
}
