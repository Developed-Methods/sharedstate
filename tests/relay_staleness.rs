//! Reproduces the relay freeze described in the 2026-09-13 downtime analysis.
//! Real cluster tasks and protocol framing run over the simulated network.
//! Network cuts model closed leader connections, without reproducing the WAN congestion that caused them.

use std::{io, time::Duration};

use message_encoding::MessageEncoding;
use sharedstate::{
    SharedState, SharedStateConfig, SharedStateSettings,
    cluster::leader::LeaderMode,
    state::deterministic_state::DeterministicState,
    transport::simulated::{SimulatedIo, SimulatedNet},
};
use tokio::time::{Instant, sleep};

const LEADER: u64 = 1;
const FOLLOWERS: [u64; 3] = [2, 3, 4];
const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
const STALE_WINDOW: Duration = Duration::from_secs(130);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct State {
    seq: u64,
    value: u64,
}

impl DeterministicState for State {
    type Action = u64;
    type AuthorityAction = u64;

    fn accept_seq(&self) -> u64 {
        self.seq
    }

    fn authority(&self, action: u64) -> u64 {
        action
    }

    fn update(&mut self, action: &u64) {
        self.seq += 1;
        self.value = *action;
    }
}

impl MessageEncoding for State {
    fn write_to<W: io::Write>(&self, out: &mut W) -> io::Result<usize> {
        Ok(self.seq.write_to(out)? + self.value.write_to(out)?)
    }

    fn read_from<R: io::Read>(read: &mut R) -> io::Result<Self> {
        Ok(Self {
            seq: u64::read_from(read)?,
            value: u64::read_from(read)?,
        })
    }
}

type Node = SharedState<SimulatedIo, State>;

async fn start_node(net: &SimulatedNet, address: u64) -> Node {
    SharedState::start(SharedStateConfig {
        io: net.start_io(address).await,
        my_address: address,
        can_lead: address == LEADER,
        initial_peers: [LEADER, 2, 3, 4].into_iter().filter(|peer| *peer != address).collect(),
        initial_state: State::default(),
        settings: SharedStateSettings::default(),
    })
    .unwrap()
}

fn snapshot(node: &Node) -> State {
    node.state_handle().read_with(|state| state.state().clone())
}

async fn wait_for_value(node: &Node, value: u64) {
    let deadline = Instant::now() + SETUP_TIMEOUT;
    loop {
        let state = snapshot(node);
        if state.value == value {
            return;
        }
        assert!(Instant::now() < deadline, "node {} did not receive value {value}; state={state:?}", node.my_address(),);
        sleep(Duration::from_millis(50)).await;
    }
}

async fn assert_same_leader(nodes: &[Node]) {
    for node in nodes {
        let state = node.leader_state().await;
        let expected = if node.my_address() == LEADER {
            LeaderMode::Leading
        } else {
            LeaderMode::Following { leader: LEADER }
        };
        assert_eq!(state.mode, expected, "node {} changed leader", node.my_address());
    }
}

struct Scenario {
    net: SimulatedNet,
    nodes: Vec<Node>,
    probe: Node,
    frozen: State,
}

async fn restore_leader_after_relay_partition() -> Scenario {
    let net = SimulatedNet::new();
    let mut nodes = Vec::new();
    for address in [LEADER, 2, 3, 4] {
        nodes.push(start_node(&net, address).await);
    }
    nodes[0].submit_action(1).await.unwrap();
    for node in &nodes {
        wait_for_value(node, 1).await;
    }
    assert_same_leader(&nodes).await;

    // Nodes 3 and 4 lose direct access; node 2 retains the last live leader feed.
    for address in [3, 4] {
        net.set_edge_blocked(LEADER, address, true).await;
    }
    nodes[0].submit_action(2).await.unwrap();
    for node in &nodes {
        wait_for_value(node, 2).await;
    }
    // Receiving this update proves the disconnected followers can consume relayed state.
    let frozen = snapshot(&nodes[1]);
    assert_eq!(frozen.seq, 2);

    // Close the last leader feed while leaving every follower-to-follower edge open.
    net.set_edge_blocked(LEADER, 2, true).await;
    sleep(Duration::from_secs(30)).await;
    assert_same_leader(&nodes).await;

    for address in FOLLOWERS {
        net.set_edge_blocked(LEADER, address, false).await;
    }
    assert!(net.topology_snapshot().await.blocked_edges.is_empty());

    // A new follower proves the restored leader accepts subscriptions and serves current state.
    let probe = start_node(&net, 5).await;
    nodes[0].submit_action(3).await.unwrap();
    wait_for_value(&probe, 3).await;
    Scenario {
        net,
        nodes,
        probe,
        frozen,
    }
}

#[tokio::test(start_paused = true)]
async fn relay_followers_remain_stale_after_leader_connectivity_returns() {
    let scenario = restore_leader_after_relay_partition().await;

    // Transport keepalives continue, but authoritative updates never reach the original followers.
    // Publish once per second, like the application's periodic expiry actions.
    for tick in 0..STALE_WINDOW.as_secs() {
        let value = 4 + tick;
        scenario.nodes[0].submit_action(value).await.unwrap();
        sleep(Duration::from_secs(1)).await;
        wait_for_value(&scenario.probe, value).await;
        assert_same_leader(&scenario.nodes).await;
        for node in &scenario.nodes[1..] {
            assert_eq!(snapshot(node), scenario.frozen, "node {} unexpectedly recovered", node.my_address());
        }
    }

    // Closing the relay connections forces resubscription and proves the frozen nodes can still recover.
    for (a, b) in [(2, 3), (2, 4), (3, 4)] {
        scenario.net.set_edge_blocked(a, b, true).await;
    }
    let current = snapshot(&scenario.nodes[0]);
    for node in &scenario.nodes[1..] {
        wait_for_value(node, current.value).await;
        assert_eq!(snapshot(node), current);
    }
}

// This assertion describes the desired behavior and intentionally fails before an implementation fix.
#[tokio::test(start_paused = true)]
#[ignore = "known relay staleness bug; run explicitly to demonstrate failed recovery"]
async fn relay_followers_should_catch_up_after_leader_connectivity_returns() {
    let scenario = restore_leader_after_relay_partition().await;
    sleep(STALE_WINDOW).await;
    let current = snapshot(&scenario.nodes[0]);
    let states: Vec<_> = scenario.nodes[1..].iter().map(snapshot).collect();
    assert!(
        states.iter().all(|state| *state == current),
        "followers did not recover within {STALE_WINDOW:?}: leader={current:?}, followers={states:?}"
    );
}
