//! Reproduces the 2026-09-13 outage where tunnel servers ended up serving
//! stale state for 90 minutes after a 6 minute network blip in the core.
//!
//! Production shape: the sync-servers (can_lead) live in the core, the
//! tunnel servers (observers, can_lead = false) are spread across regions
//! and know each other as peers. During the blip every tunnel server lost
//! its connection to the leader. Each one then relayed through another
//! tunnel server, forming chains that never led back to the leader. Once
//! the network healed nothing brought them back, because a silent relay
//! feed never ends the subscription.
//!
//! Nodes 1 and 2 are the sync-servers, 3 to 5 are the tunnel servers. The
//! blip is simulated by blocking every sync-server <-> tunnel-server edge
//! while tunnel servers can still reach each other.

use std::{
    collections::BTreeMap,
    io,
    time::{Duration, Instant},
};

use message_encoding::MessageEncoding;
use sharedstate::{
    cluster::{
        leader::{LeaderMode, LeaderTiming},
        peer_discovery::PeerDiscoveryTiming,
        state_sync::StateSyncTiming,
    },
    state::deterministic_state::DeterministicState,
    transport::{
        channels::NetIoSettings,
        simulated::{SimulatedIo, SimulatedNet},
    },
    SharedState, SharedStateConfig, SharedStateSettings,
};

const SYNC_SERVERS: [u64; 2] = [1, 2];
const TUNNEL_SERVERS: [u64; 3] = [3, 4, 5];
const LEADER: u64 = 1;

/// How long the blip lasts once every tunnel server is on a relay. Long
/// relative to the timeouts below, like 6 minutes was in production.
const BLIP_HOLD: Duration = Duration::from_secs(3);

/// How long tunnel servers get to catch up after the network heals. In
/// production they had 80 minutes and never did.
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(20);

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

/// Production timings scaled down. Ratios matter more than absolutes: the
/// message timeout bounds connect and handshake attempts, the retry delay
/// paces follow() when no source is reachable.
fn fast_settings() -> SharedStateSettings {
    SharedStateSettings {
        net: NetIoSettings {
            process_timeout: Duration::from_millis(500),
            message_timeout: Duration::from_secs(1),
        },
        broadcast: Default::default(),
        discovery_timing: PeerDiscoveryTiming {
            observation_interval: Duration::from_millis(50),
            max_concurrent_observations: 8,
        },
        leader_timing: LeaderTiming {
            tick_interval: Duration::from_millis(25),
        },
        sync_timing: StateSyncTiming {
            leader_poll_interval: Duration::from_millis(20),
            retry_delay: Duration::from_millis(50),
        },
        peer_expiry: Default::default(),
    }
}

async fn start_node(net: &SimulatedNet, address: u64, can_lead: bool) -> Node {
    let peers = SYNC_SERVERS
        .iter()
        .chain(TUNNEL_SERVERS.iter())
        .copied()
        .filter(|peer| *peer != address)
        .collect();
    let io = net.start_io(address).await;
    SharedState::start(SharedStateConfig {
        io,
        my_address: address,
        can_lead,
        voter_gateway: None,
        initial_peers: peers,
        initial_state: KvState::default(),
        settings: fast_settings(),
    })
    .unwrap()
}

async fn wait_for_value(node: &Node, key: u64, value: u64, timeout: Duration) -> Result<(), String> {
    let mut handle = node.state_handle();
    let deadline = Instant::now() + timeout;
    loop {
        if handle.read_with(|state| state.state().values.get(&key) == Some(&value)) {
            return Ok(());
        }
        if deadline <= Instant::now() {
            let seen = handle.read_with(|state| state.state().clone());
            return Err(format!(
                "node {} never saw {key}={value} within {timeout:?}, its state is seq {} {:?}",
                node.my_address(),
                seen.seq,
                seen.values,
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_leader(nodes: &[&Node], leader: u64) {
    for node in nodes {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let state = node.leader_state().await;
            let settled = match &state.mode {
                LeaderMode::Leading => node.my_address() == leader,
                LeaderMode::Following { leader: followed } => *followed == leader,
                _ => false,
            };
            if settled {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node {} never settled on leader {leader}, last state {state:?}",
                node.my_address(),
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

struct Cluster {
    net: SimulatedNet,
    sync_servers: Vec<Node>,
    tunnel_servers: Vec<Node>,
}

impl Cluster {
    fn leader(&self) -> &Node {
        self.sync_servers
            .iter()
            .find(|node| node.my_address() == LEADER)
            .unwrap()
    }

    fn all(&self) -> Vec<&Node> {
        self.sync_servers.iter().chain(self.tunnel_servers.iter()).collect()
    }

    async fn set_core_links_blocked(&self, blocked: bool) {
        for sync_server in SYNC_SERVERS {
            for tunnel_server in TUNNEL_SERVERS {
                self.net.set_edge_blocked(sync_server, tunnel_server, blocked).await;
            }
        }
    }

    /// Connections ever opened between tunnel servers, per edge. Cumulative
    /// rather than live: since the relay fix a tunnel server that tries
    /// another tunnel server as relay is told NotSynced and hangs up within
    /// milliseconds, so a live count would miss the attempt between polls.
    async fn tunnel_edge_connections_opened(&self) -> BTreeMap<(u64, u64), usize> {
        let mut counts = BTreeMap::new();
        for a in TUNNEL_SERVERS {
            for b in TUNNEL_SERVERS {
                if a < b {
                    counts.insert((a, b), self.net.edge_connections_opened(a, b).await);
                }
            }
        }
        counts
    }

    /// Waits until every tunnel server has opened at least one new
    /// connection to another tunnel server since `baseline`. Tunnel servers
    /// have no reason to talk to each other except to relay, so this is the
    /// relay fallback kicking in.
    async fn wait_for_tunnel_relays(&self, baseline: &BTreeMap<(u64, u64), usize>) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let counts = self.tunnel_edge_connections_opened().await;
            let relaying = TUNNEL_SERVERS.iter().all(|tunnel| {
                counts
                    .iter()
                    .any(|((a, b), count)| (a == tunnel || b == tunnel) && baseline[&(*a, *b)] < *count)
            });
            if relaying {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "tunnel servers never tried relaying through each other, tunnel edge connections opened {counts:?} (baseline {baseline:?})"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Brings the cluster up, runs the core blip until every tunnel server is
/// relaying through another tunnel server, then heals the network.
///
/// Returns with the network healthy and the sync-servers holding a state
/// the tunnel servers have not seen (key 2).
async fn cluster_after_core_blip() -> Cluster {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    let net = SimulatedNet::new();
    let mut sync_servers = Vec::new();
    for address in SYNC_SERVERS {
        sync_servers.push(start_node(&net, address, true).await);
    }
    let mut tunnel_servers = Vec::new();
    for address in TUNNEL_SERVERS {
        tunnel_servers.push(start_node(&net, address, false).await);
    }
    let cluster = Cluster {
        net,
        sync_servers,
        tunnel_servers,
    };

    wait_for_leader(&cluster.all(), LEADER).await;

    /* healthy replication before the blip */
    cluster.leader().submit_action((1, 1)).await.unwrap();
    for node in cluster.all() {
        wait_for_value(node, 1, 1, Duration::from_secs(10)).await.unwrap();
    }

    /* the core WAN link saturates: nothing between the sync-servers and the
     * tunnel servers gets through, existing connections die */
    let baseline = cluster.tunnel_edge_connections_opened().await;
    cluster.set_core_links_blocked(true).await;

    cluster.wait_for_tunnel_relays(&baseline).await;
    tokio::time::sleep(BLIP_HOLD).await;

    /* tunnel servers still believe node 1 leads, which is why they accept
     * each other as relays (and what bgp-manager saw as Following) */
    for tunnel_server in &cluster.tunnel_servers {
        let state = tunnel_server.leader_state().await;
        assert!(
            matches!(state.mode, LeaderMode::Following { leader: LEADER }),
            "tunnel server {} stopped following the leader during the blip: {state:?}",
            tunnel_server.my_address(),
        );
    }

    /* the leader keeps producing updates during the blip, like ClearOld
     * every second in production; the sync-servers see them, the tunnel
     * servers cannot */
    cluster.leader().submit_action((2, 2)).await.unwrap();
    for sync_server in &cluster.sync_servers {
        wait_for_value(sync_server, 2, 2, Duration::from_secs(10)).await.unwrap();
    }
    for tunnel_server in &cluster.tunnel_servers {
        assert!(
            wait_for_value(tunnel_server, 2, 2, Duration::from_millis(500)).await.is_err(),
            "tunnel server {} received leader updates while cut off from the core",
            tunnel_server.my_address(),
        );
    }

    /* the network heals; the leader is reachable and would accept anyone */
    cluster.set_core_links_blocked(false).await;

    cluster
}

/// After the blip, tunnel servers must pick the leader's feed back up and
/// catch up on everything they missed.
#[tokio::test(flavor = "multi_thread")]
async fn tunnel_servers_resync_from_leader_after_core_blip() {
    let cluster = cluster_after_core_blip().await;

    cluster.leader().submit_action((3, 3)).await.unwrap();

    let mut stale = Vec::new();
    for tunnel_server in &cluster.tunnel_servers {
        if let Err(error) = wait_for_value(tunnel_server, 3, 3, RECOVERY_TIMEOUT).await {
            stale.push(error);
        }
    }

    assert!(
        stale.is_empty(),
        "tunnel servers stayed on their relay chain after the network healed:\n{}",
        stale.join("\n"),
    );
}

/// After the blip, actions submitted on a tunnel server (register commands
/// in production) must reach the leader again.
#[tokio::test(flavor = "multi_thread")]
async fn tunnel_server_actions_reach_leader_after_core_blip() {
    let cluster = cluster_after_core_blip().await;

    for tunnel_server in &cluster.tunnel_servers {
        let key = 100 + tunnel_server.my_address();
        tunnel_server.submit_action((key, key)).await.unwrap();
    }

    let mut lost = Vec::new();
    for tunnel_server in &cluster.tunnel_servers {
        let key = 100 + tunnel_server.my_address();
        if let Err(error) = wait_for_value(cluster.leader(), key, key, RECOVERY_TIMEOUT).await {
            lost.push(error);
        }
    }

    assert!(
        lost.is_empty(),
        "tunnel server actions never reached the leader after the network healed:\n{}",
        lost.join("\n"),
    );
}
