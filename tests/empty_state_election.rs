//! Three sync-servers start together with no clients. Two of them come up
//! with an empty state (sequence 0), each under a new state identity, and
//! one kept the cluster's state. The one holding the data must lead: a
//! node that follows a leader it cannot recover from resets to the
//! leader's state, so electing an empty node wipes the data cluster-wide.

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

/// A and B restarted empty; C kept the data. C has the highest address so
/// the lowest-address tiebreak works against it.
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

async fn assert_data_holder_leads(kept: fn(u64) -> RecoverableState<KvState>) {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    for attempt in 0..ATTEMPTS {
        let net = SimulatedNet::new();
        /* identities must differ per node and per attempt, like a restart */
        let base = 1_000 * (attempt as u64 + 1);
        let mut nodes = BTreeMap::new();
        nodes.insert(A, start_node(&net, A, empty(base + A)).await);
        nodes.insert(B, start_node(&net, B, empty(base + B)).await);
        nodes.insert(C, start_node(&net, C, kept(base + C)).await);

        let leader = wait_for_common_leader(&nodes).await;
        let mut sequences = BTreeMap::new();
        for (address, node) in &nodes {
            sequences.insert(*address, node.state_handle().read_with(|state| state.state().seq));
        }
        assert_eq!(
            leader, C,
            "attempt {attempt}: node {leader} with an empty state was elected over C, sequences now {sequences:?}"
        );

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
    assert_data_holder_leads(kept_snapshot).await;
}

/// C kept the recoverable state it held in the cluster.
#[tokio::test(flavor = "multi_thread")]
async fn node_holding_cluster_history_is_elected_over_empty_nodes() {
    assert_data_holder_leads(kept_history).await;
}
