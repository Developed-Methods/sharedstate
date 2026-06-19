use super::*;
use crate::net::simulated::{SimulatedNet, SimulatedTopologySnapshot};
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    env,
    sync::atomic::Ordering,
    time::Duration,
};

#[derive(Clone, Debug)]
struct FuzzyConfig {
    can_lead_count: usize,
    follower_count: usize,
    run_for: Duration,
    event_interval: Duration,
    settle_timeout: Duration,
    seed: u64,
}

impl FuzzyConfig {
    fn smoke() -> Self {
        Self {
            can_lead_count: 3,
            follower_count: 3,
            run_for: Duration::from_secs(5),
            event_interval: Duration::from_millis(100),
            settle_timeout: Duration::from_secs(2),
            seed: 1,
        }
    }

    fn long_from_env() -> Self {
        Self {
            can_lead_count: env_usize("SHAREDSTATE_FUZZ_CAN_LEAD", 3),
            follower_count: env_usize("SHAREDSTATE_FUZZ_FOLLOWERS", 5),
            run_for: Duration::from_secs(env_u64("SHAREDSTATE_FUZZ_SECONDS", 300)),
            event_interval: Duration::from_millis(env_u64("SHAREDSTATE_FUZZ_EVENT_MS", 250)),
            settle_timeout: Duration::from_secs(env_u64("SHAREDSTATE_FUZZ_SETTLE_SECONDS", 10)),
            seed: env_u64("SHAREDSTATE_FUZZ_SEED", default_runtime_seed()),
        }
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn default_runtime_seed() -> u64 {
    now_ms() ^ PROMOTION_COUNTER.fetch_add(1, Ordering::SeqCst).rotate_left(19)
}

#[allow(dead_code)]
struct FuzzyCluster {
    net: SimulatedNet,
    nodes: HashMap<u64, TestClusterNode>,
    can_lead: BTreeSet<u64>,
    followers: BTreeSet<u64>,
    all_addresses: Vec<u64>,
}

impl FuzzyCluster {
    async fn start(config: &FuzzyConfig) -> Self {
        assert!(config.can_lead_count > 0, "fuzzy test needs at least one can_lead node");

        let net = SimulatedNet::new();
        let can_lead = (0..config.can_lead_count)
            .map(|idx| 7001 + idx as u64)
            .collect::<BTreeSet<_>>();
        let followers = (0..config.follower_count)
            .map(|idx| 7001 + config.can_lead_count as u64 + idx as u64)
            .collect::<BTreeSet<_>>();
        let all_addresses = can_lead.iter().chain(followers.iter()).copied().collect::<Vec<_>>();
        let mut cluster = Self {
            net,
            nodes: HashMap::new(),
            can_lead,
            followers,
            all_addresses,
        };

        for address in cluster.all_addresses.clone() {
            cluster.start_one(address).await;
        }

        cluster
    }

    async fn start_one(&mut self, address: u64) {
        let peers = self
            .all_addresses
            .iter()
            .copied()
            .filter(|peer| *peer != address)
            .collect::<Vec<_>>();
        let node = start_cluster_node(&self.net, address, self.can_lead.contains(&address), &peers).await;
        self.nodes.insert(address, node);
    }

    async fn stop_node(&mut self, address: u64) {
        if let Some(node) = self.nodes.remove(&address) {
            node.stop(&self.net).await;
        } else {
            self.net.stop_node(address).await;
        }
    }

    async fn restart_node(&mut self, address: u64) {
        self.stop_node(address).await;
        self.start_one(address).await;
    }

    async fn block_edge(&self, a: u64, b: u64) {
        self.net.set_edge_blocked(a, b, true).await;
    }

    async fn unblock_edge(&self, a: u64, b: u64) {
        self.net.set_edge_blocked(a, b, false).await;
    }

    async fn isolate_node(&self, address: u64) {
        self.net.set_node_blocked(address, true).await;
    }

    async fn heal_node(&self, address: u64) {
        self.net.set_node_blocked(address, false).await;
    }

    async fn heal_all_network(&self) {
        self.net.clear_edge_blocks().await;
        self.net.clear_node_blocks().await;
    }

    async fn restart_all_nodes(&mut self) {
        let existing = std::mem::take(&mut self.nodes);
        for (_, node) in existing {
            node.stop(&self.net).await;
        }

        for address in self.all_addresses.clone() {
            self.start_one(address).await;
        }
    }

    async fn online_addresses(&self) -> BTreeSet<u64> {
        self.nodes.keys().copied().collect()
    }

    async fn debug_snapshot(&self) -> Vec<NodeDebugInfo<u64>> {
        let mut snapshot = Vec::new();
        for address in &self.all_addresses {
            if let Some(node) = self.nodes.get(address) {
                snapshot.push(node.node.debug_info().await);
            }
        }
        snapshot
    }

    async fn current_expectation(&self) -> (SimulatedTopologySnapshot, StabilizationExpectation) {
        let topology = self.net.topology_snapshot().await;
        let expectation = stabilization_expectation(&topology, &self.can_lead);
        (topology, expectation)
    }

    async fn apply_event(&mut self, event: FuzzyEvent) {
        match event {
            FuzzyEvent::StopNode(address) => self.stop_node(address).await,
            FuzzyEvent::RestartNode(address) => self.restart_node(address).await,
            FuzzyEvent::BlockEdge(a, b) => self.block_edge(a, b).await,
            FuzzyEvent::UnblockEdge(a, b) => self.unblock_edge(a, b).await,
            FuzzyEvent::IsolateNode(address) => self.isolate_node(address).await,
            FuzzyEvent::HealNode(address) => self.heal_node(address).await,
            FuzzyEvent::ClearEdgeBlocks => self.net.clear_edge_blocks().await,
            FuzzyEvent::HealAllNetwork => self.heal_all_network().await,
            FuzzyEvent::KillCurrentLeader => {
                if let Some(leader) = self.agreed_or_reported_leader().await {
                    self.stop_node(leader).await;
                }
            }
            FuzzyEvent::RestartOfflineCanLead => {
                let online = self.online_addresses().await;
                if let Some(address) = self.can_lead.iter().find(|address| !online.contains(address)).copied() {
                    self.restart_node(address).await;
                }
            }
        }
    }

    async fn agreed_or_reported_leader(&self) -> Option<u64> {
        let debug = self.debug_snapshot().await;
        let mut counts = HashMap::<u64, u64>::new();
        for info in debug {
            if let Some(leader) = info.leader {
                *counts.entry(leader).or_default() += 1;
            }
        }

        counts
            .into_iter()
            .max_by(|(a_leader, a_count), (b_leader, b_count)| {
                a_count.cmp(b_count).then_with(|| b_leader.cmp(a_leader))
            })
            .map(|(leader, _)| leader)
    }
}

#[derive(Clone, Debug)]
enum FuzzyEvent {
    StopNode(u64),
    RestartNode(u64),
    BlockEdge(u64, u64),
    UnblockEdge(u64, u64),
    IsolateNode(u64),
    HealNode(u64),
    ClearEdgeBlocks,
    HealAllNetwork,
    KillCurrentLeader,
    RestartOfflineCanLead,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
enum StabilizationExpectation {
    Stabilizable {
        online: BTreeSet<u64>,
        possible_leaders: BTreeSet<u64>,
    },
    NotStabilizable {
        reason: NotStabilizableReason,
        online: BTreeSet<u64>,
        online_can_lead: BTreeSet<u64>,
    },
}

#[derive(Clone, Debug)]
enum NotStabilizableReason {
    NoOnlineCanLead,
    Partitioned,
}

#[derive(Debug)]
#[allow(dead_code)]
struct FuzzyFailureReport {
    seed: u64,
    elapsed: Duration,
    event_index: u64,
    last_events: Vec<FuzzyEvent>,
    topology: SimulatedTopologySnapshot,
    expectation: StabilizationExpectation,
    debug: Vec<NodeDebugInfo<u64>>,
}

fn stabilization_expectation(
    topology: &SimulatedTopologySnapshot,
    can_lead: &BTreeSet<u64>,
) -> StabilizationExpectation {
    let online = topology
        .online
        .difference(&topology.blocked_nodes)
        .copied()
        .collect::<BTreeSet<_>>();
    let online_can_lead = online.intersection(can_lead).copied().collect::<BTreeSet<_>>();

    if online_can_lead.is_empty() {
        return StabilizationExpectation::NotStabilizable {
            reason: NotStabilizableReason::NoOnlineCanLead,
            online,
            online_can_lead,
        };
    }

    let Some(start) = online.iter().next().copied() else {
        return StabilizationExpectation::NotStabilizable {
            reason: NotStabilizableReason::NoOnlineCanLead,
            online,
            online_can_lead,
        };
    };

    let mut visited = BTreeSet::new();
    let mut queue = VecDeque::from([start]);
    while let Some(current) = queue.pop_front() {
        if !visited.insert(current) {
            continue;
        }

        for next in online.iter().copied() {
            if current == next || visited.contains(&next) {
                continue;
            }
            if !topology.blocked_edges.contains(&SimulatedNet::edge_key(current, next)) {
                queue.push_back(next);
            }
        }
    }

    if visited == online {
        StabilizationExpectation::Stabilizable {
            online,
            possible_leaders: online_can_lead,
        }
    } else {
        StabilizationExpectation::NotStabilizable {
            reason: NotStabilizableReason::Partitioned,
            online,
            online_can_lead,
        }
    }
}

async fn wait_for_common_leader(
    cluster: &FuzzyCluster,
    expected: &StabilizationExpectation,
    timeout: Duration,
    report_context: &FuzzyReportContext<'_>,
) -> Result<u64, FuzzyFailureReport> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(leader) = common_leader(cluster, expected).await {
            return Ok(leader);
        }

        if deadline <= tokio::time::Instant::now() {
            return Err(report_context.report(cluster, expected.clone()).await);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn common_leader(cluster: &FuzzyCluster, expected: &StabilizationExpectation) -> Option<u64> {
    let StabilizationExpectation::Stabilizable {
        online,
        possible_leaders,
    } = expected
    else {
        return Some(0);
    };

    let topology = cluster.net.topology_snapshot().await;
    let usable = online
        .difference(&topology.blocked_nodes)
        .copied()
        .collect::<BTreeSet<_>>();
    let mut leader = None;
    let mut debug_by_address = HashMap::new();
    for info in cluster.debug_snapshot().await {
        if !usable.contains(&info.address) {
            continue;
        }
        let current = info.leader?;
        if !possible_leaders.contains(&current) {
            return None;
        }
        if leader.replace(current).is_some_and(|existing| existing != current) {
            return None;
        }
        debug_by_address.insert(info.address, info);
    }

    let leader = leader?;
    let leader_info = debug_by_address.get(&leader)?;
    if leader_info.leader != Some(leader)
        || leader_info.leader_path.as_deref() != Some(&[leader])
        || leader_info.follow_remote.is_some()
    {
        return None;
    }

    for (address, info) in debug_by_address {
        let Some(path) = info.leader_path else {
            return None;
        };
        if path.is_empty() || path.first().copied() != Some(leader) {
            return None;
        }
        let mut seen = BTreeSet::new();
        if path.iter().any(|step| !seen.insert(*step)) {
            return None;
        }
        if address != leader
            && path
                .iter()
                .take(path.len().saturating_sub(1))
                .any(|step| *step == address)
        {
            return None;
        }
    }

    Some(leader)
}

struct FuzzyReportContext<'a> {
    seed: u64,
    started: tokio::time::Instant,
    event_index: u64,
    last_events: &'a VecDeque<FuzzyEvent>,
    topology: SimulatedTopologySnapshot,
}

impl FuzzyReportContext<'_> {
    async fn report(&self, cluster: &FuzzyCluster, expectation: StabilizationExpectation) -> FuzzyFailureReport {
        FuzzyFailureReport {
            seed: self.seed,
            elapsed: self.started.elapsed(),
            event_index: self.event_index,
            last_events: self.last_events.iter().cloned().collect(),
            topology: self.topology.clone(),
            expectation,
            debug: cluster.debug_snapshot().await,
        }
    }
}

fn remember_event(events: &mut VecDeque<FuzzyEvent>, event: FuzzyEvent) {
    events.push_back(event);
    while 50 < events.len() {
        events.pop_front();
    }
}

async fn choose_event(rng: &mut ChaCha8Rng, cluster: &FuzzyCluster) -> FuzzyEvent {
    let roll = rng.next_u64() % 100;
    if roll < 20 {
        return FuzzyEvent::StopNode(random_from_set(rng, &cluster.online_addresses().await).unwrap_or(7001));
    }
    if roll < 40 {
        let online = cluster.online_addresses().await;
        let offline = cluster
            .all_addresses
            .iter()
            .copied()
            .filter(|address| !online.contains(address))
            .collect::<BTreeSet<_>>();
        return FuzzyEvent::RestartNode(random_from_set(rng, &offline).unwrap_or(7001));
    }
    if roll < 60 {
        let (a, b) = random_pair(rng, &cluster.all_addresses).unwrap_or((7001, 7002));
        return FuzzyEvent::BlockEdge(a, b);
    }
    if roll < 75 {
        let topology = cluster.net.topology_snapshot().await;
        if let Some((a, b)) = random_from_set(rng, &topology.blocked_edges) {
            return FuzzyEvent::UnblockEdge(a, b);
        }
        let (a, b) = random_pair(rng, &cluster.all_addresses).unwrap_or((7001, 7002));
        return FuzzyEvent::BlockEdge(a, b);
    }
    if roll < 85 {
        return FuzzyEvent::IsolateNode(random_from_set(rng, &cluster.online_addresses().await).unwrap_or(7001));
    }
    if roll < 90 {
        let topology = cluster.net.topology_snapshot().await;
        return FuzzyEvent::HealNode(random_from_set(rng, &topology.blocked_nodes).unwrap_or(7001));
    }
    if roll < 95 {
        return FuzzyEvent::ClearEdgeBlocks;
    }

    match rng.next_u64() % 5 {
        0 => FuzzyEvent::KillCurrentLeader,
        1 => {
            let debug = cluster.debug_snapshot().await;
            let path_edges = debug
                .iter()
                .filter_map(|info| info.leader_path.as_ref())
                .flat_map(|path| {
                    path.windows(2)
                        .map(|edge| SimulatedNet::edge_key(edge[0], edge[1]))
                        .collect::<Vec<_>>()
                })
                .collect::<BTreeSet<_>>();
            if let Some((a, b)) = random_from_set(rng, &path_edges) {
                FuzzyEvent::BlockEdge(a, b)
            } else {
                let (a, b) = random_pair(rng, &cluster.all_addresses).unwrap_or((7001, 7002));
                FuzzyEvent::BlockEdge(a, b)
            }
        }
        2 => FuzzyEvent::IsolateNode(random_from_set(rng, &cluster.can_lead).unwrap_or(7001)),
        3 => FuzzyEvent::RestartOfflineCanLead,
        _ => FuzzyEvent::HealAllNetwork,
    }
}

fn random_from_set<T: Copy + Ord>(rng: &mut ChaCha8Rng, set: &BTreeSet<T>) -> Option<T> {
    if set.is_empty() {
        return None;
    }
    let index = (rng.next_u64() as usize) % set.len();
    set.iter().nth(index).copied()
}

fn random_pair(rng: &mut ChaCha8Rng, addresses: &[u64]) -> Option<(u64, u64)> {
    if addresses.len() < 2 {
        return None;
    }
    let a = (rng.next_u64() as usize) % addresses.len();
    let mut b = (rng.next_u64() as usize) % addresses.len();
    if a == b {
        b = (b + 1) % addresses.len();
    }
    Some(SimulatedNet::edge_key(addresses[a], addresses[b]))
}

async fn run_fuzzy_cluster(config: FuzzyConfig) {
    let mut rng = ChaCha8Rng::seed_from_u64(config.seed);
    let mut cluster = FuzzyCluster::start(&config).await;
    let started = tokio::time::Instant::now();
    let mut event_index = 0;
    let mut last_events = VecDeque::new();

    let (topology, expectation) = cluster.current_expectation().await;
    if matches!(expectation, StabilizationExpectation::Stabilizable { .. }) {
        let context = FuzzyReportContext {
            seed: config.seed,
            started,
            event_index,
            last_events: &last_events,
            topology,
        };
        if let Err(report) = wait_for_common_leader(&cluster, &expectation, config.settle_timeout, &context).await {
            panic!("fuzzy cluster failed to stabilize:\n{report:#?}");
        }
    }

    while started.elapsed() < config.run_for {
        let event = choose_event(&mut rng, &cluster).await;
        cluster.apply_event(event.clone()).await;
        remember_event(&mut last_events, event);
        tokio::time::sleep(config.event_interval).await;

        let (topology, expectation) = cluster.current_expectation().await;
        if matches!(expectation, StabilizationExpectation::Stabilizable { .. }) {
            let context = FuzzyReportContext {
                seed: config.seed,
                started,
                event_index,
                last_events: &last_events,
                topology,
            };
            if let Err(report) = wait_for_common_leader(&cluster, &expectation, config.settle_timeout, &context).await {
                panic!("fuzzy cluster failed to stabilize:\n{report:#?}");
            }
        } else {
            tracing::debug!(?expectation, "fuzzy topology is not globally stabilizable");
        }

        event_index += 1;
    }

    cluster.heal_all_network().await;
    cluster.restart_all_nodes().await;
    tokio::time::sleep(config.event_interval).await;

    let (topology, expectation) = cluster.current_expectation().await;
    let context = FuzzyReportContext {
        seed: config.seed,
        started,
        event_index,
        last_events: &last_events,
        topology,
    };
    if let Err(report) = wait_for_common_leader(&cluster, &expectation, config.settle_timeout, &context).await {
        panic!("fuzzy cluster failed to stabilize:\n{report:#?}");
    }
}

#[tokio::test]
async fn fuzzy_cluster_stabilization_smoke() {
    run_fuzzy_cluster(FuzzyConfig::smoke()).await;
}

#[tokio::test]
#[ignore]
async fn fuzzy_cluster_stabilization_long() {
    run_fuzzy_cluster(FuzzyConfig::long_from_env()).await;
}
