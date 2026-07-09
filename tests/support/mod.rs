#![allow(dead_code)]

use std::{
    collections::{BTreeMap, HashMap},
    io,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use message_encoding::MessageEncoding;
use sharedstate::{
    state::{deterministic_state::DeterministicState, recoverable_state::RecoverableState},
    transport::traits::{SyncConnection, SyncIO, SyncIOListener},
    DebugInfo, SharedState, SharedStateConfig, SharedStateSettings,
};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpListener, TcpStream,
};

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

#[derive(Clone, Debug, Default)]
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

pub(crate) fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();
}

pub(crate) struct TestCluster {
    leader_address: Mutex<u16>,
    nodes: Vec<SharedState<LocalhostTcpIo, KvState>>,
    node_by_address: HashMap<u16, usize>,
}

impl TestCluster {
    pub(crate) async fn start(size: usize) -> Self {
        assert_ne!(size, 0, "test cluster must have at least one node");

        let mut ios = Vec::with_capacity(size);
        for _ in 0..size {
            ios.push(LocalhostTcpIo::bind_ephemeral().await.unwrap());
        }

        let initial_leader = ios[0].address;
        let available_peers = ios.iter().map(|io| io.address).collect::<Vec<_>>();
        let mut nodes = Vec::with_capacity(size);
        let mut node_by_address = HashMap::with_capacity(size);

        for io in ios {
            let index = nodes.len();
            node_by_address.insert(io.address, index);
            nodes.push(Self::start_node(io, initial_leader, available_peers.clone()));
        }

        Self {
            leader_address: Mutex::new(initial_leader),
            nodes,
            node_by_address,
        }
    }

    fn start_node(
        io: LocalhostTcpIo,
        leader_address: u16,
        available_peers: Vec<u16>,
    ) -> SharedState<LocalhostTcpIo, KvState> {
        let my_address = io.address;
        SharedState::start(SharedStateConfig {
            io: Arc::new(io),
            my_address,
            leader_address,
            available_peers,
            initial_state: RecoverableState::new(my_address as u64, KvState::default()),
            settings: SharedStateSettings::default(),
        })
        .unwrap()
    }

    pub(crate) fn address(&self, index: usize) -> u16 {
        self.nodes[index].my_address()
    }

    pub(crate) fn leader_address(&self) -> u16 {
        let leader_address = *self.leader_address.lock().unwrap();
        assert!(self.node_by_address.contains_key(&leader_address), "leader should be a known test node");
        leader_address
    }

    pub(crate) fn is_leader(&self, index: usize) -> bool {
        self.nodes[index].is_leader()
    }

    pub(crate) fn debug_info(&self, index: usize) -> DebugInfo<u16> {
        self.nodes[index].debug_info()
    }

    pub(crate) fn elect(&self, index: usize) {
        let leader_address = self.address(index);
        *self.leader_address.lock().unwrap() = leader_address;
        for node in &self.nodes {
            node.set_leader_address(leader_address);
        }
    }

    pub(crate) async fn submit(&self, index: usize, key: &str, value: &str) {
        self.nodes[index]
            .submit_action((key.to_owned(), value.to_owned()))
            .await
            .unwrap();
    }

    pub(crate) async fn submit_until_all_apply(&self, index: usize, key: &str, value: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            self.submit(index, key, value).await;

            let retry_at = Instant::now() + Duration::from_millis(250);
            loop {
                if self.all_have_value(key, value) {
                    return;
                }
                assert!(Instant::now() < deadline, "timed out waiting for all nodes to apply {key}={value}");
                if retry_at <= Instant::now() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }

    pub(crate) async fn wait_connected_to_leader(&self, index: usize) {
        self.wait_until(
            || self.nodes[index].is_connected_to_leader(),
            format!("node {} never connected to leader", self.address(index)),
        )
        .await;
    }

    pub(crate) async fn wait_all_connected_to_leader(&self) {
        for index in 0..self.nodes.len() {
            self.wait_connected_to_leader(index).await;
        }
    }

    pub(crate) async fn wait_leader(&self, index: usize) {
        self.wait_until(|| self.nodes[index].is_leader(), format!("node {} never became leader", self.address(index)))
            .await;
    }

    pub(crate) async fn wait_not_leader(&self, index: usize) {
        self.wait_until(|| !self.nodes[index].is_leader(), format!("node {} never stepped down", self.address(index)))
            .await;
    }

    pub(crate) async fn wait_value(&self, index: usize, key: &str, value: &str) {
        let node = &self.nodes[index];
        let mut handle = node.state_handle();
        self.wait_until(
            || handle.read_with(|state| state.state().values.get(key).map(String::as_str) == Some(value)),
            format!("node {} never saw {key}={value}", node.my_address()),
        )
        .await;
    }

    pub(crate) async fn wait_all_for_value(&self, key: &str, value: &str) {
        for index in 0..self.nodes.len() {
            self.wait_value(index, key, value).await;
        }
    }

    pub(crate) async fn wait_accept_seq(&self, index: usize, min_seq: u64) {
        let node = &self.nodes[index];
        let mut handle = node.state_handle();
        self.wait_until(
            || handle.read_with(|state| state.accept_seq() >= min_seq),
            format!("node {} never reached accept sequence {min_seq}", node.my_address()),
        )
        .await;
    }

    pub(crate) fn assert_all_contain_keys(&self, keys: &[&str]) {
        for key in keys {
            for node in &self.nodes {
                let mut handle = node.state_handle();
                assert!(
                    handle.read_with(|state| state.state().values.contains_key(*key)),
                    "node {} is missing {key}",
                    node.my_address(),
                );
            }
        }
    }

    fn all_have_value(&self, key: &str, value: &str) -> bool {
        self.nodes.iter().all(|node| {
            let mut handle = node.state_handle();
            handle.read_with(|state| state.state().values.get(key).map(String::as_str) == Some(value))
        })
    }

    async fn wait_until(&self, mut condition: impl FnMut() -> bool, failure: String) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if condition() {
                return;
            }
            assert!(Instant::now() < deadline, "{failure}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
