//! Three sync-servers start together with no clients. Two of them come up
//! with an empty state (sequence 0), each under a new state identity, or
//! with a fork of the cluster's history that fell behind, and one holds
//! the most actions. That one must lead: a node that follows a leader it
//! cannot recover from resets to the leader's state, so electing another
//! node discards its actions cluster-wide.

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
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction},
    },
    transport::{
        channels::NetIoSettings,
        simulated::{SimulatedIo, SimulatedNet},
    },
    SharedState, SharedStateRecoverableConfig, SharedStateSettings,
};

/// C holds the most actions and has the highest address, so the
/// lowest-address tiebreak works against it.
const A: u64 = 1;
const B: u64 = 2;
const C: u64 = 3;
const NODES: [u64; 3] = [A, B, C];

/// How many actions C's state holds.
const KEPT_SEQUENCE: u64 = 1_000;

/// Fresh clusters to elect in; the election races discovery, so one run
/// can come out right by luck.
const ATTEMPTS: usize = 5;

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

/// A state with `KEPT_SEQUENCE` actions applied, as loaded from a snapshot
/// of the deterministic state under a new identity.
fn kept_snapshot(id: u64) -> RecoverableState<KvState> {
    let state = KvState {
        seq: KEPT_SEQUENCE,
        values: (0..KEPT_SEQUENCE).map(|key| (key, key)).collect(),
    };
    RecoverableState::new(id, state)
}

/// A state with `KEPT_SEQUENCE` actions applied under a leader, as loaded
/// from the recoverable state a node saved while in the cluster.
fn kept_history(id: u64) -> RecoverableState<KvState> {
    let mut state = RecoverableState::new(id, KvState::default());
    state.update(&RecoverableStateAction::BumpGeneration { new_id: id + 1 });
    for key in 0..KEPT_SEQUENCE {
        state.update(&RecoverableStateAction::StateAction { action: (key, key) });
    }
    state
}

fn empty(id: u64) -> RecoverableState<KvState> {
    RecoverableState::new(id, KvState::default())
}

/// Applies actions for `keys` under a new leader with id `leader_id`.
fn lead(state: &mut RecoverableState<KvState>, leader_id: u64, keys: std::ops::Range<u64>) {
    state.update(&RecoverableStateAction::BumpGeneration { new_id: leader_id });
    for key in keys {
        state.update(&RecoverableStateAction::StateAction { action: (key, key) });
    }
}

/// The cluster's history up to sequence 400, then split: A and B stayed
/// with an old leader that reached sequence 500, C's side elected a new
/// leader and reached `KEPT_SEQUENCE`.
fn forked(base: u64) -> [RecoverableState<KvState>; 3] {
    let mut common = empty(base);
    lead(&mut common, base + 10, 0..400);
    let mut behind = common.clone();
    lead(&mut behind, base + 20, 400..500);
    let mut ahead = common;
    lead(&mut ahead, base + 30, 400..KEPT_SEQUENCE);
    [behind.clone(), behind, ahead]
}

async fn start_node(net: &SimulatedNet, address: u64, initial_state: RecoverableState<KvState>) -> Node {
    let io = net.start_io(address).await;
    SharedState::start_recoverable(SharedStateRecoverableConfig {
        io,
        my_address: address,
        can_lead: true,
        accessible: true,
        voter_gateway: None,
        initial_peers: NODES.into_iter().filter(|peer| *peer != address).collect(),
        initial_state,
        settings: fast_settings(),
    })
    .unwrap()
}

async fn wait_for_common_leader(nodes: &BTreeMap<u64, Node>) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut leaders = Vec::new();
        for (address, node) in nodes {
            leaders.push(match node.leader_state().await.mode {
                LeaderMode::Leading => Some(*address),
                LeaderMode::Following { leader } => Some(leader),
                _ => None,
            });
        }
        if let Some(leader) = leaders[0]
            && leaders.iter().all(|seen| *seen == Some(leader))
        {
            return leader;
        }
        assert!(Instant::now() < deadline, "nodes never agreed on a leader: {leaders:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// `states` gives A, B and C their initial states for an attempt; ids
/// must differ per attempt, like a restart.
async fn assert_data_holder_leads(states: fn(u64) -> [RecoverableState<KvState>; 3]) {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    for attempt in 0..ATTEMPTS {
        let net = SimulatedNet::new();
        let base = 1_000 * (attempt as u64 + 1);
        let mut nodes = BTreeMap::new();
        for (address, state) in NODES.into_iter().zip(states(base)) {
            nodes.insert(address, start_node(&net, address, state).await);
        }

        let leader = wait_for_common_leader(&nodes).await;
        let mut sequences = BTreeMap::new();
        for (address, node) in &nodes {
            sequences.insert(*address, node.state_handle().read_with(|state| state.state().seq));
        }
        assert_eq!(
            leader, C,
            "attempt {attempt}: node {leader} with fewer actions was elected over C, sequences now {sequences:?}"
        );

        /* everyone ends up with C's state */
        let deadline = Instant::now() + Duration::from_secs(10);
        for (address, node) in &nodes {
            let mut handle = node.state_handle();
            while handle.read_with(|state| state.state().seq) != KEPT_SEQUENCE {
                assert!(Instant::now() < deadline, "attempt {attempt}: node {address} never took C's state");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        for address in NODES {
            net.stop_node(address).await;
        }
    }
}

/// C loaded the deterministic state into a new recoverable state, so its
/// recovery details look like those of an empty state apart from the
/// sequence.
#[tokio::test(flavor = "multi_thread")]
async fn node_holding_a_snapshot_is_elected_over_empty_nodes() {
    assert_data_holder_leads(|base| [empty(base + A), empty(base + B), kept_snapshot(base + C)]).await;
}

/// C kept the recoverable state it held in the cluster.
#[tokio::test(flavor = "multi_thread")]
async fn node_holding_cluster_history_is_elected_over_empty_nodes() {
    assert_data_holder_leads(|base| [empty(base + A), empty(base + B), kept_history(base + C)]).await;
}

/// A and B hold a fork of the cluster's history that fell behind C's, so
/// they can recover each other but not C, and C cannot replay onto them.
#[tokio::test(flavor = "multi_thread")]
async fn node_holding_the_longer_fork_is_elected_over_a_shared_shorter_fork() {
    assert_data_holder_leads(forked).await;
}
