//! Fault-recovery integration tests over the simulated transport.

use std::{
    collections::BTreeMap,
    io, iter,
    time::{Duration, Instant},
};

use message_encoding::MessageEncoding;
use sharedstate::{
    cluster::state_sync::StateSyncTiming,
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails},
    },
    transport::{channels::NetIoSettings, simulated::SimulatedNet},
    SharedState, SharedStateConfig, SharedStateSettings,
};
use tokio::sync::watch;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct KvState {
    seq: u64,
    values: BTreeMap<String, String>,
}

impl DeterministicState for KvState {
    type Action = (String, String);
    type AuthorityAction = (String, String);

    fn accept_seq(&self) -> u64 {
        self.seq
    }

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
        action
    }

    fn update(&mut self, (key, value): &Self::AuthorityAction) {
        self.values.insert(key.clone(), value.clone());
        self.seq += 1;
    }
}

impl MessageEncoding for KvState {
    fn write_to<T: io::Write>(&self, out: &mut T) -> io::Result<usize> {
        let mut written = self.seq.write_to(out)?;
        written += (self.values.len() as u64).write_to(out)?;
        for (key, value) in &self.values {
            written += key.write_to(out)?;
            written += value.write_to(out)?;
        }
        Ok(written)
    }

    fn read_from<T: io::Read>(read: &mut T) -> io::Result<Self> {
        let seq = u64::read_from(read)?;
        let len = u64::read_from(read)? as usize;
        let mut values = BTreeMap::new();
        for _ in 0..len {
            values.insert(String::read_from(read)?, String::read_from(read)?);
        }
        Ok(Self { seq, values })
    }
}

struct SimCluster {
    net: SimulatedNet,
    leader_tx: watch::Sender<u64>,
    nodes: Vec<SharedState<sharedstate::transport::simulated::SimulatedIo, KvState>>,
}

impl SimCluster {
    async fn start(size: usize) -> Self {
        assert_ne!(size, 0);

        let net = SimulatedNet::new();
        let initial_leader = 1;
        let (leader_tx, leader_rx) = watch::channel(initial_leader);
        let mut nodes = Vec::with_capacity(size);

        for address in 1..=size as u64 {
            let io = net.start_io(address).await;
            nodes.push(
                SharedState::start(SharedStateConfig {
                    io,
                    my_address: address,
                    leader_address: leader_rx.clone(),
                    initial_state: RecoverableState::new(address, KvState::default()),
                    settings: test_settings(),
                })
                .unwrap(),
            );
        }

        Self { net, leader_tx, nodes }
    }

    fn leader(&self) -> &SharedState<sharedstate::transport::simulated::SimulatedIo, KvState> {
        &self.nodes[0]
    }

    fn assert_leader_address(&self) {
        assert_eq!(*self.leader_tx.borrow(), 1);
    }

    async fn submit_to_leader(&self, key: &str, value: &str) {
        self.leader()
            .submit_action((key.to_owned(), value.to_owned()))
            .await
            .unwrap();
    }

    async fn bump_leader_generation(&self, new_id: u64) {
        self.leader()
            .node()
            .state
            .update(iter::once(RecoverableStateAction::BumpGeneration { new_id }))
            .await;
    }

    async fn wait_connected(&self, index: usize) {
        wait_until(
            || self.nodes[index].is_connected_to_leader(),
            format!("node {} never connected to leader", self.nodes[index].my_address()),
        )
        .await;
    }

    async fn wait_disconnected(&self, index: usize) {
        wait_until(
            || !self.nodes[index].is_connected_to_leader(),
            format!("node {} never disconnected from leader", self.nodes[index].my_address()),
        )
        .await;
    }

    async fn wait_value(&self, index: usize, key: &str, value: &str) {
        let node = &self.nodes[index];
        let mut handle = node.state_handle();
        wait_until(
            || handle.read_with(|state| state.state().values.get(key).map(String::as_str) == Some(value)),
            format!("node {} never saw {key}={value}", node.my_address()),
        )
        .await;
    }

    async fn wait_details_equal(&self, left: usize, right: usize) {
        let mut left_handle = self.nodes[left].state_handle();
        let mut right_handle = self.nodes[right].state_handle();
        wait_until(
            || left_handle.recover_details() == right_handle.recover_details(),
            format!(
                "node {} recovery details never matched node {}",
                self.nodes[left].my_address(),
                self.nodes[right].my_address()
            ),
        )
        .await;
    }

    fn recovery_details(&self, index: usize) -> RecoverableStateDetails {
        self.nodes[index].state_handle().recover_details()
    }
}

fn test_settings() -> SharedStateSettings {
    SharedStateSettings {
        net: NetIoSettings {
            process_timeout: Duration::from_millis(100),
            message_timeout: Duration::from_millis(300),
        },
        broadcast: Default::default(),
        sync_timing: StateSyncTiming {
            retry_delay: Duration::from_millis(50),
        },
    }
}

async fn wait_until(mut condition: impl FnMut() -> bool, failure: String) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if condition() {
            return;
        }
        assert!(Instant::now() < deadline, "{failure}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn blocked_edge_disconnects_then_recovers_and_converges() {
    let cluster = SimCluster::start(2).await;
    cluster.assert_leader_address();
    cluster.wait_connected(1).await;

    cluster.net.set_edge_blocked(1, 2, true).await;
    cluster.wait_disconnected(1).await;

    cluster.submit_to_leader("during-block", "leader-value").await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    cluster.net.set_edge_blocked(1, 2, false).await;

    cluster.wait_connected(1).await;
    cluster.wait_value(1, "during-block", "leader-value").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn blackholed_edge_times_out_retries_then_converges_after_heal() {
    let cluster = SimCluster::start(2).await;
    cluster.assert_leader_address();
    cluster.wait_connected(1).await;

    cluster.net.set_edge_blackholed(1, 2, true).await;
    cluster.submit_to_leader("during-blackhole", "leader-value").await;

    cluster.wait_disconnected(1).await;

    cluster.net.set_edge_blackholed(1, 2, false).await;

    cluster.wait_connected(1).await;
    cluster.wait_value(1, "during-blackhole", "leader-value").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_gets_fresh_state_when_generation_history_is_unavailable() {
    const RETAINED_GENERATIONS: u64 = 2048;

    let cluster = SimCluster::start(2).await;
    cluster.assert_leader_address();
    cluster.wait_connected(1).await;
    cluster.submit_to_leader("before-gap", "visible").await;
    cluster.wait_value(1, "before-gap", "visible").await;

    cluster.net.set_edge_blocked(1, 2, true).await;
    cluster.wait_disconnected(1).await;
    let stale_follower_details = cluster.recovery_details(1);

    for offset in 0..=RETAINED_GENERATIONS {
        cluster.bump_leader_generation(10_000 + offset).await;
    }
    cluster.submit_to_leader("after-gap", "fresh").await;

    let leader_details = cluster.recovery_details(0);
    assert!(
        !leader_details.can_recover_follower(&stale_follower_details),
        "test setup must force the leader to send FreshState instead of replaying retained history"
    );

    cluster.net.set_edge_blocked(1, 2, false).await;

    cluster.wait_connected(1).await;
    cluster.wait_value(1, "after-gap", "fresh").await;
    cluster.wait_details_equal(0, 1).await;
}
