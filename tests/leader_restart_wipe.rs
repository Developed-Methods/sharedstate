//! Reproduces the 2026-11-01 outage where a rolling reboot of the
//! sync-servers wiped the tunnel state on every node.
//!
//! The sync-servers (voters) were rebooted one at a time for a kernel
//! update. The leader came back within seconds with an empty state and,
//! before it learned about its peers, elected itself as a lone voter.
//! Production starts the node with no initial peers and seeds them right
//! after, so the first leader tick can run against an empty peer map.
//!
//! The other voters and the tunnel servers still followed that address in
//! the cluster's current term. Its new process accepted their subscriptions
//! as the leader of its own term, could not recover their states, and sent
//! each of them its empty state as a fresh snapshot. The leader then learned
//! the real term and stepped down, but every node had already been reset.
//! The tunnel servers dropped 99% of their tunnels and traffic fell by 75%
//! until the tunnel-state-updater had rebuilt the state from the database.
//!
//! Nodes 1 to 3 are the sync-servers, 4 and 5 are the tunnel servers. The
//! tunnel servers either reach the sync-servers directly or only through a
//! load balancer in front of them (the voter gateway).

use std::{
    collections::BTreeMap,
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use message_encoding::MessageEncoding;
use sharedstate::{
    cluster::{
        leader::{LeaderMode, LeaderTiming},
        node_state::{ConnectStatus, PeerState},
        peer_discovery::PeerDiscoveryTiming,
        state_sync::StateSyncTiming,
    },
    protocol::messages::ElectionTerm,
    state::deterministic_state::DeterministicState,
    transport::{
        channels::NetIoSettings,
        simulated::{SimulatedIo, SimulatedNet},
    },
    SharedState, SharedStateConfig, SharedStateSettings,
};

const SYNC_SERVERS: [u64; 3] = [1, 2, 3];
const TUNNEL_SERVERS: [u64; 2] = [4, 5];

/// Keys written before the restart. Every node must still hold them after.
const KEYS: u64 = 50;

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

/// Production timings scaled down by 10x. The ratio that matters here is
/// discovery (3s in production) against the sync retry (500ms): a follower
/// re-dials its leader several times before gossip tells it anything new.
fn fast_settings() -> SharedStateSettings {
    SharedStateSettings {
        net: NetIoSettings {
            process_timeout: Duration::from_millis(500),
            message_timeout: Duration::from_secs(1),
        },
        broadcast: Default::default(),
        discovery_timing: PeerDiscoveryTiming {
            observation_interval: Duration::from_millis(300),
            max_concurrent_observations: 8,
            gateway_view_ttl: Duration::from_secs(5),
        },
        leader_timing: LeaderTiming {
            tick_interval: Duration::from_millis(50),
            ..Default::default()
        },
        sync_timing: StateSyncTiming {
            leader_poll_interval: Duration::from_millis(20),
            retry_delay: Duration::from_millis(50),
            ..Default::default()
        },
        peer_expiry: Default::default(),
        min_leader_peers: 0,
    }
}

fn is_sync_server(address: u64) -> bool {
    SYNC_SERVERS.contains(&address)
}

fn all_addresses() -> impl Iterator<Item = u64> {
    SYNC_SERVERS.into_iter().chain(TUNNEL_SERVERS)
}

/// The load balancer in front of the sync-servers.
const GATEWAY: u64 = 100;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Topology {
    /// The tunnel servers dial the sync-servers directly.
    Direct,
    /// The tunnel servers only reach the sync-servers through the gateway,
    /// and the sync-servers cannot dial them.
    Gateway,
}

fn behind_gateway(address: u64, topology: Topology) -> bool {
    topology == Topology::Gateway && !is_sync_server(address)
}

async fn start_node(
    net: &SimulatedNet,
    address: u64,
    initial_peers: Vec<u64>,
    topology: Topology,
    settings: SharedStateSettings,
) -> Node {
    let io = net.start_io(address).await;
    let gateway = behind_gateway(address, topology);
    SharedState::start(SharedStateConfig {
        io,
        my_address: address,
        can_lead: is_sync_server(address),
        accessible: true,
        voter_gateway: gateway.then_some(GATEWAY),
        initial_peers: if gateway { Vec::new() } else { initial_peers },
        initial_state: KvState::default(),
        settings,
    })
    .unwrap()
}

/// What tunnel-sync's `discover_peer_details` does right after starting
/// the node: adds the static peer list, voters marked as such.
async fn seed_peers(node: &Node, topology: Topology) {
    let mut peers = node.node().peers.lock().await;
    for address in all_addresses().filter(|address| *address != node.my_address() && !behind_gateway(*address, topology)) {
        peers
            .entry(address)
            .and_modify(|peer| peer.can_lead = Some(is_sync_server(address)))
            .or_insert(PeerState {
                addr: address,
                can_lead: Some(is_sync_server(address)),
                connect_status: ConnectStatus::NotConnected,
                last_global_connectivity: None,
                leader_info: None,
            });
    }
}

async fn wait_for_common_leader(nodes: &[&Node]) -> u64 {
    wait_for_common_leader_other_than(nodes, None).await
}

/// The leader every sync-server and directly connected tunnel server agrees
/// on, with the tunnel servers behind the gateway following the gateway.
async fn wait_for_common_leader_other_than(nodes: &[&Node], old: Option<u64>) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut leaders = Vec::new();
        for node in nodes {
            leaders.push(match node.leader_state().await.mode {
                LeaderMode::Leading => Some(node.my_address()),
                LeaderMode::Following { leader } => Some(leader),
                _ => None,
            });
        }
        let gateway_followers = nodes
            .iter()
            .zip(&leaders)
            .filter(|(node, seen)| node.node().voter_gateway.is_some() && **seen == Some(GATEWAY))
            .count();
        let gateway_observers = nodes.iter().filter(|node| node.node().voter_gateway.is_some()).count();
        let direct = leaders.iter().filter(|seen| **seen != Some(GATEWAY)).collect::<Vec<_>>();
        if let Some(Some(leader)) = direct.first().copied() {
            if Some(*leader) != old && direct.iter().all(|seen| **seen == Some(*leader)) && gateway_followers == gateway_observers {
                return *leader;
            }
        }
        assert!(Instant::now() < deadline, "nodes never agreed on a leader: {leaders:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_keys(node: &Node, keys: u64, timeout: Duration) -> Result<(), String> {
    let mut handle = node.state_handle();
    let deadline = Instant::now() + timeout;
    loop {
        if handle.read_with(|state| (0..keys).all(|key| state.state().values.get(&key) == Some(&key))) {
            return Ok(());
        }
        if deadline <= Instant::now() {
            let len = handle.read_with(|state| state.state().values.len());
            return Err(format!(
                "node {} holds {len} of {keys} keys after {timeout:?}",
                node.my_address()
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn start_cluster(net: &SimulatedNet, topology: Topology) -> BTreeMap<u64, Node> {
    if topology == Topology::Gateway {
        net.add_gateway(GATEWAY, SYNC_SERVERS.to_vec()).await;
        for voter in SYNC_SERVERS {
            for observer in TUNNEL_SERVERS {
                net.set_dial_blocked(voter, observer, true).await;
            }
        }
    }
    let mut nodes = BTreeMap::new();
    for address in all_addresses() {
        let peers = all_addresses().filter(|peer| *peer != address).collect();
        nodes.insert(address, start_node(net, address, peers, topology, fast_settings()).await);
    }
    nodes
}

/// Starts a cluster that settles in its first term. Every process starts in
/// term 0 of its own root; nodes that elect before they hear of every root
/// settle in a later term, so start over until one does not.
async fn start_cluster_in_first_term(topology: Topology) -> (SimulatedNet, BTreeMap<u64, Node>, u64) {
    let mut attempts = 0;
    let (net, nodes, leader, term) = loop {
        let net = SimulatedNet::new();
        let nodes = start_cluster(&net, topology).await;
        let leader = wait_for_common_leader(&nodes.values().collect::<Vec<_>>()).await;
        let term = nodes[&leader].leader_state().await.term;
        if term.term() == 0 {
            break (net, nodes, leader, term);
        }
        attempts += 1;
        assert!(attempts < 5, "the cluster never settled in its first term");
        for address in nodes.keys() {
            net.stop_node(*address).await;
        }
    };

    /* the nonce of a new root comes from the clock in milliseconds with
     * its low bits scrambled, so it only orders reliably across a gap of
     * more than 512ms. The restarted root must outrank the cluster's for
     * the tests to model the hazard */
    tokio::time::sleep(Duration::from_secs(1)).await;
    let restarted_term = ElectionTerm::default();
    assert!(term < restarted_term, "a new root no longer outranks the cluster's: {term} vs {restarted_term}");
    (net, nodes, leader)
}

/// How the restarted leader comes back.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Restart {
    /// It elects itself alone before it hears of its peers, as it did in
    /// the outage. Since then a voter waits for its peers before it does,
    /// so this models peers that cannot reach it within that wait. When
    /// `adoptable`, its term is one the cluster adopts (see below).
    LoneClaim { adoptable: bool },
    /// It waits for its peers as it does now and must never lead alone.
    WaitForPeers,
    /// It is cut off from the other sync-servers while the load balancer
    /// still routes the tunnel servers to it, and its wait for peers runs
    /// out before it hears of them. Tunnel servers share the sync-server
    /// addresses they know through the load balancer, which usually tells
    /// it of its peers within the wait; this models it not. Only a voter
    /// that needs `min_leader_peers` other voters to back it holds back.
    CutOffFromVoters { min_leader_peers: usize },
}

/// What happened while the leader restarted.
struct RestartOutcome {
    /// The restarted node led while it held its empty state.
    led_alone: bool,
    /// Some other node lost keys at some point.
    wiped: bool,
    /// Nodes still missing keys once the cluster settled.
    missing: Vec<String>,
}

impl RestartOutcome {
    fn assert_state_kept(&self) {
        assert!(
            !self.led_alone,
            "the restarted leader led with its empty state before it heard of its peers"
        );
        assert!(
            !self.wiped && self.missing.is_empty(),
            "the restarted leader's empty state replaced the cluster's state (wiped while restarting: {}):\n{}",
            self.wiped,
            self.missing.join("\n"),
        );
    }
}

/// Rolling restart of the elected leader, the way the sync-servers were
/// rebooted for the kernel update. Production had been through many
/// elections, so the cluster's term number was well above the restarted
/// node's starting term.
#[tokio::test(flavor = "multi_thread")]
async fn restarted_leader_does_not_wipe_cluster_state() {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    let net = SimulatedNet::new();
    let mut nodes = start_cluster(&net, Topology::Direct).await;
    let first_leader = wait_for_common_leader(&nodes.values().collect::<Vec<_>>()).await;

    /* get past the initial term so the cluster's term is not the restarted
     * node's starting one */
    net.set_node_blocked(first_leader, true).await;
    net.stop_node(first_leader).await;
    drop(nodes.remove(&first_leader));
    let survivors = nodes.values().collect::<Vec<_>>();
    let leader = wait_for_common_leader_other_than(&survivors, Some(first_leader)).await;
    let term = nodes[&leader].leader_state().await.term;
    assert!(0 < term.term(), "failover did not move past the first term: {term}");
    net.set_node_blocked(first_leader, false).await;
    let restarted = start_node(&net, first_leader, Vec::new(), Topology::Direct, fast_settings()).await;
    seed_peers(&restarted, Topology::Direct).await;
    nodes.insert(first_leader, restarted);
    assert_eq!(wait_for_common_leader(&nodes.values().collect::<Vec<_>>()).await, leader);

    restart_leader_and_check_keys(&net, nodes, leader, Topology::Direct, Restart::LoneClaim { adoptable: false })
        .await
        .assert_state_kept();
}

/// The same restart in a cluster that never had a failover. A restarted
/// process picks a newer root that outranks the cluster's at the same term
/// number, so the other nodes adopt its term instead of seeing it as stale.
#[tokio::test(flavor = "multi_thread")]
async fn restarted_leader_does_not_wipe_cluster_state_in_first_term() {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    let (net, nodes, leader) = start_cluster_in_first_term(Topology::Direct).await;
    restart_leader_and_check_keys(&net, nodes, leader, Topology::Direct, Restart::LoneClaim { adoptable: true })
        .await
        .assert_state_kept();
}

/// The restart as it plays out now, with the tunnel servers behind the
/// load balancer: they only see whichever sync-server answers, so they
/// cannot tell a restarted one's lone claim from the cluster's. The
/// restarted leader must not make one.
#[tokio::test(flavor = "multi_thread")]
async fn restarted_leader_waits_for_its_peers_before_leading_alone() {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    let (net, nodes, leader) = start_cluster_in_first_term(Topology::Gateway).await;
    restart_leader_and_check_keys(&net, nodes, leader, Topology::Gateway, Restart::WaitForPeers)
        .await
        .assert_state_kept();
}

/// Waiting for peers does not help a restarted leader that cannot reach
/// them in time: once the wait is over it leads alone, and the load
/// balancer hands its empty state to the tunnel servers. This is the
/// hazard `min_leader_peers` closes, kept here so the next test keeps
/// modelling it.
#[tokio::test(flavor = "multi_thread")]
async fn restarted_leader_cut_off_from_its_peers_leads_alone_without_a_peer_minimum() {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    let (net, nodes, leader) = start_cluster_in_first_term(Topology::Gateway).await;
    let outcome = restart_leader_and_check_keys(
        &net,
        nodes,
        leader,
        Topology::Gateway,
        Restart::CutOffFromVoters { min_leader_peers: 0 },
    )
    .await;
    assert!(outcome.led_alone, "the restarted leader never led alone, the test no longer models the hazard");
}

/// With a peer minimum the restarted leader waits, however long it stays
/// cut off, until another sync-server backs it, and the others only back
/// the sync-server holding the cluster's state.
#[tokio::test(flavor = "multi_thread")]
async fn restarted_leader_cut_off_from_its_peers_waits_for_a_peer_minimum() {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    let (net, nodes, leader) = start_cluster_in_first_term(Topology::Gateway).await;
    restart_leader_and_check_keys(
        &net,
        nodes,
        leader,
        Topology::Gateway,
        Restart::CutOffFromVoters { min_leader_peers: 1 },
    )
    .await
    .assert_state_kept();
}

async fn restart_leader_and_check_keys(
    net: &SimulatedNet,
    mut nodes: BTreeMap<u64, Node>,
    leader: u64,
    topology: Topology,
    restart: Restart,
) -> RestartOutcome {
    for key in 0..KEYS {
        nodes[&leader].submit_action((key, key)).await.unwrap();
    }
    for node in nodes.values() {
        wait_for_keys(node, KEYS, Duration::from_secs(10)).await.unwrap();
    }

    /* every node other than the leader must keep the keys while the leader
     * reboots; sample often so a reset that is later repaired still shows */
    let wiped = Arc::new(AtomicBool::new(false));
    let mut watchers = nodes
        .values()
        .filter(|node| node.my_address() != leader)
        .map(|node| {
            let address = node.my_address();
            let mut handle = node.state_handle();
            let wiped = wiped.clone();
            tokio::spawn(async move {
                loop {
                    let len = handle.read_with(|state| state.state().values.len());
                    if (len as u64) < KEYS && !wiped.swap(true, Ordering::SeqCst) {
                        tracing::error!(address, len, "node lost its state while the leader restarted");
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
        })
        .collect::<Vec<_>>();

    /* in the worst case the others learn of a lone claim before the
     * restarted node learns of their term: it pushes its state to the nodes
     * it was seeded with before they push theirs on their next discovery
     * round, which in production can be up to 3s later. Where its term is
     * one they could adopt, hold their dials back until they have reacted
     * to its claim */
    let cluster_term = nodes[&leader].leader_state().await.term;
    let adoptable = restart == Restart::LoneClaim { adoptable: true };
    if adoptable {
        for node in nodes.keys() {
            net.set_dial_blocked(*node, leader, true).await;
        }
    }

    let other_sync_servers = SYNC_SERVERS.into_iter().filter(|address| *address != leader).collect::<Vec<_>>();
    if let Restart::CutOffFromVoters { .. } = restart {
        for address in &other_sync_servers {
            net.set_edge_blocked(leader, *address, true).await;
        }
    }

    /* the leader reboots: its process dies and comes back moments later
     * with an empty state. Production passes no initial peers and seeds them
     * after start */
    net.stop_node(leader).await;
    drop(nodes.remove(&leader));
    let mut settings = fast_settings();
    match restart {
        Restart::LoneClaim { .. } => settings.leader_timing.lone_leader_grace = Some(Duration::ZERO),
        Restart::WaitForPeers => {}
        Restart::CutOffFromVoters { min_leader_peers } => {
            settings.leader_timing.lone_leader_grace = Some(Duration::ZERO);
            settings.min_leader_peers = min_leader_peers;
        }
    }
    let restarted = start_node(net, leader, Vec::new(), topology, settings).await;

    let led_alone = Arc::new(AtomicBool::new(false));
    if matches!(restart, Restart::WaitForPeers | Restart::CutOffFromVoters { .. }) {
        let state = restarted.node().clone();
        let led_alone = led_alone.clone();
        watchers.push(tokio::spawn(async move {
            loop {
                if matches!(state.leader_state.lock().await.mode, LeaderMode::Leading)
                    && !led_alone.swap(true, Ordering::SeqCst)
                {
                    tracing::error!("restarted node leads with its empty state");
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }));
    }

    if restart == Restart::WaitForPeers {
        /* seeding lost the race against the first leader ticks in the
         * outage; let a few run, still well inside the wait for peers */
        tokio::time::sleep(fast_settings().leader_timing.tick_interval * 3).await;
        seed_peers(&restarted, topology).await;
    } else if let Restart::CutOffFromVoters { .. } = restart {
        /* stay cut off for several discovery rounds while the tunnel
         * servers keep dialing the load balancer, which routes every third
         * connection to the restarted node */
        tokio::time::sleep(fast_settings().discovery_timing.observation_interval * 8).await;
        for address in &other_sync_servers {
            net.set_edge_blocked(leader, *address, false).await;
        }
        seed_peers(&restarted, topology).await;
    } else {
        /* wait for the lone claim before seeding so the test does not
         * depend on winning the race against the first leader tick */
        let deadline = Instant::now() + Duration::from_secs(5);
        while !matches!(restarted.leader_state().await.mode, LeaderMode::Leading) {
            assert!(
                Instant::now() < deadline,
                "restarted node never claimed leadership on its own, the test no longer models the outage"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        seed_peers(&restarted, topology).await;
    }

    if adoptable {
        let deadline = Instant::now() + Duration::from_secs(5);
        'reacted: loop {
            for node in nodes.values() {
                if node.leader_state().await.term == cluster_term {
                    assert!(
                        Instant::now() < deadline,
                        "node {} never reacted to the restarted node's claim",
                        node.my_address()
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue 'reacted;
                }
            }
            break;
        }
        for node in nodes.keys() {
            net.set_dial_blocked(*node, leader, false).await;
        }
    }
    nodes.insert(leader, restarted);

    let new_leader = wait_for_common_leader(&nodes.values().collect::<Vec<_>>()).await;
    tracing::info!(new_leader, "cluster settled after the leader restart");

    let mut missing = Vec::new();
    for node in nodes.values() {
        if let Err(error) = wait_for_keys(node, KEYS, Duration::from_secs(10)).await {
            missing.push(error);
        }
    }
    for watcher in watchers {
        watcher.abort();
    }

    RestartOutcome {
        led_alone: led_alone.load(Ordering::SeqCst),
        wiped: wiped.load(Ordering::SeqCst),
        missing,
    }
}
