//! End-to-end fixed-leader sync test over real TCP.

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
    sync::watch,
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

async fn start_node(io: LocalhostTcpIo, leader_address: u16) -> SharedState<LocalhostTcpIo, KvState> {
    let (_leader_tx, leader_rx) = watch::channel(leader_address);
    start_node_with_leader_rx(io, leader_rx).await
}

async fn start_node_with_leader_rx(
    io: LocalhostTcpIo,
    leader_address: watch::Receiver<u16>,
) -> SharedState<LocalhostTcpIo, KvState> {
    let my_address = io.address;
    SharedState::start(SharedStateConfig {
        io: Arc::new(io),
        my_address,
        leader_address,
        initial_state: RecoverableState::new(my_address as u64, KvState::default()),
        settings: SharedStateSettings::default(),
    })
    .unwrap()
}

async fn wait_for_value(node: &SharedState<LocalhostTcpIo, KvState>, key: &str, value: &str, timeout: Duration) {
    let mut handle = node.state_handle();
    let deadline = Instant::now() + timeout;
    loop {
        if handle.read_with(|state| state.state().values.get(key).map(String::as_str) == Some(value)) {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for node {} to see {key}={value}", node.my_address(),);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_missing(node: &SharedState<LocalhostTcpIo, KvState>, key: &str, timeout: Duration) {
    let mut handle = node.state_handle();
    let deadline = Instant::now() + timeout;
    loop {
        if handle.read_with(|state| !state.state().values.contains_key(key)) {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for node {} to remove {key}", node.my_address(),);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_accept_seq(node: &SharedState<LocalhostTcpIo, KvState>, min_seq: u64, timeout: Duration) {
    let mut handle = node.state_handle();
    let deadline = Instant::now() + timeout;
    loop {
        if handle.read_with(|state| state.accept_seq() >= min_seq) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for node {} to reach accept sequence {min_seq}",
            node.my_address(),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_actions_apply_through_fixed_leader_over_tcp() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    let leader_io = LocalhostTcpIo::bind_ephemeral().await.unwrap();
    let follower_io = LocalhostTcpIo::bind_ephemeral().await.unwrap();
    let leader_address = leader_io.address;

    let leader = start_node(leader_io, leader_address).await;
    let follower = start_node(follower_io, leader_address).await;

    let deadline = Instant::now() + Duration::from_secs(10);
    while !follower.is_connected_to_leader() {
        assert!(Instant::now() < deadline, "follower never connected to leader");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    follower
        .submit_action(("from-follower".to_owned(), "1".to_owned()))
        .await
        .unwrap();
    wait_for_value(&leader, "from-follower", "1", Duration::from_secs(10)).await;
    wait_for_value(&follower, "from-follower", "1", Duration::from_secs(10)).await;

    leader
        .submit_action(("from-leader".to_owned(), "2".to_owned()))
        .await
        .unwrap();
    wait_for_value(&follower, "from-leader", "2", Duration::from_secs(10)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn node_switches_between_following_and_leading_when_leader_address_changes() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    let original_leader_io = LocalhostTcpIo::bind_ephemeral().await.unwrap();
    let switching_io = LocalhostTcpIo::bind_ephemeral().await.unwrap();
    let original_leader_address = original_leader_io.address;
    let switching_address = switching_io.address;

    let original_leader = start_node(original_leader_io, original_leader_address).await;
    let (leader_tx, leader_rx) = watch::channel(original_leader_address);
    let switching = start_node_with_leader_rx(switching_io, leader_rx).await;

    let deadline = Instant::now() + Duration::from_secs(10);
    while !switching.is_connected_to_leader() {
        assert!(Instant::now() < deadline, "switching node never connected to original leader");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!switching.is_leader());

    leader_tx.send(switching_address).unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    while !switching.is_leader() {
        assert!(Instant::now() < deadline, "switching node never became leader");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    wait_for_accept_seq(&switching, 3, Duration::from_secs(10)).await;

    switching
        .submit_action(("while-leader".to_owned(), "1".to_owned()))
        .await
        .unwrap();
    wait_for_value(&switching, "while-leader", "1", Duration::from_secs(10)).await;

    leader_tx.send(original_leader_address).unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    while switching.is_leader() {
        assert!(Instant::now() < deadline, "switching node never stepped down");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    wait_for_missing(&switching, "while-leader", Duration::from_secs(10)).await;

    switching
        .submit_action(("after-step-down".to_owned(), "2".to_owned()))
        .await
        .unwrap();
    wait_for_value(&original_leader, "after-step-down", "2", Duration::from_secs(10)).await;
    wait_for_value(&switching, "after-step-down", "2", Duration::from_secs(10)).await;
}
