//! End-to-end failover test over real TCP with default settings, mirroring
//! the kv_tui example's wiring.

use std::{
    collections::BTreeMap,
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use message_encoding::MessageEncoding;
use sharedstate::{
    cluster::leader::LeaderMode,
    state::deterministic_state::DeterministicState,
    transport::traits::{SyncConnection, SyncIO, SyncIOListener},
    SharedState, SharedStateConfig, SharedStateSettings,
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

async fn start_node(io: LocalhostTcpIo, can_lead: bool, peers: &[u16]) -> SharedState<LocalhostTcpIo, KvState> {
    let my_address = io.address;
    SharedState::start(SharedStateConfig {
        io: Arc::new(io),
        my_address,
        can_lead,
        initial_peers: peers.to_vec(),
        initial_state: KvState::default(),
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
        assert!(
            Instant::now() < deadline,
            "timed out waiting for node {} to see {key}={value}",
            node.my_address(),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_settled_leader(
    nodes: &[&SharedState<LocalhostTcpIo, KvState>],
    leader: u16,
    timeout: Duration,
) {
    for node in nodes {
        let deadline = Instant::now() + timeout;
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
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_actions_apply_after_leader_change_over_tcp() {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).try_init();

    let io1 = LocalhostTcpIo::bind_ephemeral().await.unwrap();
    let io2 = LocalhostTcpIo::bind_ephemeral().await.unwrap();
    let io3 = LocalhostTcpIo::bind_ephemeral().await.unwrap();

    let (addr1, addr2, addr3) = (io1.address, io2.address, io3.address);
    let mut order = [addr1, addr2, addr3];
    order.sort();
    let first_leader = order[0];
    let second_leader = order[1];

    /* the first leader runs on its own runtime so it can be killed like a
     * real process: every task dies and its sockets close */
    let mut ios = vec![io1, io2, io3];
    let leader_pos = ios.iter().position(|io| io.address == first_leader).unwrap();
    let leader_io = ios.remove(leader_pos);
    let leader_peers: Vec<u16> = ios.iter().map(|io| io.address).collect();

    let leader_rt = tokio::runtime::Runtime::new().unwrap();
    let leader_node = {
        let _guard = leader_rt.enter();
        SharedState::start(SharedStateConfig {
            my_address: leader_io.address,
            io: Arc::new(leader_io),
            can_lead: true,
            initial_peers: leader_peers,
            initial_state: KvState::default(),
            settings: SharedStateSettings::default(),
        })
        .unwrap()
    };

    let mut remaining = Vec::new();
    for io in ios {
        let peers: Vec<u16> = [addr1, addr2, addr3]
            .iter()
            .copied()
            .filter(|addr| *addr != io.address)
            .collect();
        remaining.push(start_node(io, true, &peers).await);
    }

    {
        let all_refs: Vec<_> = remaining.iter().chain([&leader_node]).collect();
        wait_for_settled_leader(&all_refs, first_leader, Duration::from_secs(20)).await;
    }

    remaining[0]
        .submit_action(("before".to_owned(), "1".to_owned()))
        .await
        .unwrap();
    for node in &remaining {
        wait_for_value(node, "before", "1", Duration::from_secs(20)).await;
    }

    /* kill the leader like a process exit */
    drop(leader_node);
    leader_rt.shutdown_background();

    let remaining_refs: Vec<_> = remaining.iter().collect();
    wait_for_settled_leader(&remaining_refs, second_leader, Duration::from_secs(60)).await;

    /* the follower that moved to the new leader submits an action */
    let moved_follower = remaining
        .iter()
        .find(|node| node.my_address() != second_leader)
        .unwrap();
    moved_follower
        .submit_action(("after".to_owned(), "2".to_owned()))
        .await
        .unwrap();

    for node in &remaining {
        wait_for_value(node, "after", "2", Duration::from_secs(30)).await;
    }
}
