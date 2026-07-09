//! Production-style concurrency and leader churn tests over real TCP.

use std::{
    collections::BTreeMap,
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use message_encoding::MessageEncoding;
use sharedstate::{
    state::{deterministic_state::DeterministicState, recoverable_state::RecoverableState},
    transport::traits::{SyncConnection, SyncIO, SyncIOListener},
    SharedState, SharedStateConfig, SharedStateSettings,
};
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::Mutex,
    task::JoinSet,
};

const WAIT_TIMEOUT: Duration = Duration::from_secs(12);

#[derive(Clone)]
struct LocalhostTcpIo {
    address: u16,
    listener: Arc<TcpListener>,
}

impl LocalhostTcpIo {
    async fn bind_ephemeral() -> io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?.port();
        Ok(Self {
            address,
            listener: Arc::new(listener),
        })
    }
}

impl SyncIO for LocalhostTcpIo {
    type Address = u16;
    type Read = OwnedReadHalf;
    type Write = OwnedWriteHalf;

    async fn connect(&self, remote: &Self::Address) -> io::Result<SyncConnection<Self>> {
        let stream = TcpStream::connect(("127.0.0.1", *remote)).await?;
        let (read, write) = stream.into_split();
        Ok(SyncConnection {
            remote: *remote,
            read,
            write,
        })
    }
}

impl SyncIOListener for LocalhostTcpIo {
    async fn next_client(&self) -> io::Result<SyncConnection<Self>> {
        let (stream, peer) = self.listener.accept().await?;
        let (read, write) = stream.into_split();
        Ok(SyncConnection {
            remote: peer.port(),
            read,
            write,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ExactKvState {
    seq: u64,
    values: BTreeMap<String, String>,
}

impl DeterministicState for ExactKvState {
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

impl MessageEncoding for ExactKvState {
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

struct ExactCluster {
    nodes: Vec<SharedState<LocalhostTcpIo, ExactKvState>>,
}

impl ExactCluster {
    async fn start(size: usize) -> Self {
        assert_ne!(size, 0, "test cluster must have at least one node");

        let mut ios = Vec::with_capacity(size);
        for _ in 0..size {
            ios.push(LocalhostTcpIo::bind_ephemeral().await.unwrap());
        }

        let initial_leader = ios[0].address;
        let available_peers = ios.iter().map(|io| io.address).collect::<Vec<_>>();
        let mut nodes = Vec::with_capacity(size);

        for io in ios {
            let my_address = io.address;
            nodes.push(
                SharedState::start(SharedStateConfig {
                    io: Arc::new(io),
                    my_address,
                    leader_address: initial_leader,
                    available_peers: available_peers.clone(),
                    initial_state: RecoverableState::new(my_address as u64, ExactKvState::default()),
                    settings: SharedStateSettings::default(),
                })
                .unwrap(),
            );
        }

        Self { nodes }
    }

    fn elect(&self, index: usize) {
        let leader_address = self.nodes[index].my_address();
        for node in &self.nodes {
            node.set_leader_address(leader_address);
        }
    }

    fn is_leader(&self, index: usize) -> bool {
        self.nodes[index].is_leader()
    }

    fn snapshot(&self, index: usize) -> BTreeMap<String, String> {
        let mut handle = self.nodes[index].state_handle();
        handle.read_with(|state| state.state().values.clone())
    }

    fn accept_seq(&self, index: usize) -> u64 {
        let mut handle = self.nodes[index].state_handle();
        handle.read_with(|state| state.accept_seq())
    }

    async fn submit(&self, index: usize, key: String, value: String) {
        self.nodes[index].submit_action((key, value)).await.unwrap();
    }

    async fn submit_until_all_apply(&self, index: usize, key: &str, value: &str) {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            self.submit(index, key.to_owned(), value.to_owned()).await;

            let retry_at = Instant::now() + Duration::from_millis(150);
            loop {
                if self.all_have_value(key, value) {
                    return;
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for all nodes to apply {key}={value}: {:?}",
                    self.snapshots()
                );
                if retry_at <= Instant::now() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    async fn wait_all_connected_to_leader(&self) {
        for index in 0..self.nodes.len() {
            self.wait_until(
                || self.nodes[index].is_connected_to_leader(),
                format!("node {} never connected to leader", self.nodes[index].my_address()),
            )
            .await;
        }
    }

    async fn wait_leader(&self, index: usize) {
        self.wait_until(
            || self.nodes[index].is_leader(),
            format!("node {} never became leader", self.nodes[index].my_address()),
        )
        .await;
    }

    async fn wait_all_exact(&self, expected: &BTreeMap<String, String>) {
        self.wait_until(
            || (0..self.nodes.len()).all(|index| self.snapshot(index) == *expected),
            format!("nodes did not converge to expected state: {:?}", self.snapshots()),
        )
        .await;
    }

    async fn wait_all_accept_seq_at_least(&self, min_seq: u64) {
        self.wait_until(
            || (0..self.nodes.len()).all(|index| self.accept_seq(index) >= min_seq),
            format!("nodes did not all reach accept sequence {min_seq}: {:?}", self.accept_seqs()),
        )
        .await;
    }

    fn snapshots(&self) -> Vec<BTreeMap<String, String>> {
        (0..self.nodes.len()).map(|index| self.snapshot(index)).collect()
    }

    fn all_have_value(&self, key: &str, value: &str) -> bool {
        (0..self.nodes.len()).all(|index| self.snapshot(index).get(key).map(String::as_str) == Some(value))
    }

    fn accept_seqs(&self) -> Vec<u64> {
        (0..self.nodes.len()).map(|index| self.accept_seq(index)).collect()
    }

    async fn wait_until(&self, mut condition: impl FnMut() -> bool, failure: String) {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            if condition() {
                return;
            }
            assert!(Instant::now() < deadline, "{failure}");
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_submissions_through_leader_and_followers_converge_to_exact_key_set() {
    init_tracing();

    let cluster = Arc::new(ExactCluster::start(4).await);
    cluster.wait_all_connected_to_leader().await;

    let mut expected = BTreeMap::new();
    let mut submissions = Vec::new();
    for node_index in 0..4 {
        for action_index in 0..8 {
            let key = format!("node-{node_index}-action-{action_index}");
            let value = format!("value-{node_index}-{action_index}");
            expected.insert(key.clone(), value.clone());
            submissions.push((node_index, key, value));
        }
    }

    let mut tasks = JoinSet::new();
    for (node_index, key, value) in submissions {
        let cluster = cluster.clone();
        tasks.spawn(async move {
            cluster.submit(node_index, key, value).await;
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    cluster.wait_all_exact(&expected).await;

    for index in 0..4 {
        assert_eq!(cluster.snapshot(index), expected, "node {index} has extra or missing keys");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_churn_while_submitting_actions_keeps_cluster_convergent_and_connected() {
    init_tracing();

    let cluster = Arc::new(ExactCluster::start(3).await);
    cluster.wait_all_connected_to_leader().await;

    let expected = Arc::new(Mutex::new(BTreeMap::new()));
    let mut submitters = JoinSet::new();

    for node_index in 0..3 {
        let cluster = cluster.clone();
        let expected = expected.clone();
        submitters.spawn(async move {
            for action_index in 0..6 {
                let key = format!("churn-node-{node_index}-action-{action_index}");
                let value = format!("value-{node_index}-{action_index}");
                expected.lock().await.insert(key.clone(), value.clone());
                cluster.submit_until_all_apply(node_index, &key, &value).await;
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        });
    }

    for leader_index in [1, 2, 0, 2, 1, 0] {
        cluster.elect(leader_index);
        cluster.wait_leader(leader_index).await;
        tokio::time::sleep(Duration::from_millis(35)).await;
    }

    while let Some(result) = submitters.join_next().await {
        result.unwrap();
    }

    cluster.elect(0);
    cluster.wait_leader(0).await;
    cluster.wait_all_connected_to_leader().await;

    let expected = expected.lock().await.clone();
    cluster.wait_all_exact(&expected).await;
    cluster.wait_all_accept_seq_at_least(expected.len() as u64).await;

    assert!(cluster.is_leader(0), "final elected node is not leader");
    for index in 0..3 {
        assert_eq!(cluster.snapshot(index), expected, "node {index} did not converge exactly");
    }
}
