//! Drop/shutdown behavior tests for production transports and task cleanup.

use std::{
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use message_encoding::MessageEncoding;
use sharedstate::{
    state::{deterministic_state::DeterministicState, recoverable_state::RecoverableState},
    transport::{
        channels::NetIoSettings,
        simulated::{SimulatedIo, SimulatedNet},
        traits::{SyncConnection, SyncIO, SyncIOListener},
    },
    SharedState, SharedStateConfig, SharedStateSettings,
};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpListener, TcpStream,
};

#[derive(Clone, Debug, Default)]
struct CounterState {
    seq: u64,
    sum: u64,
}

impl DeterministicState for CounterState {
    type Action = u64;
    type AuthorityAction = u64;

    fn accept_seq(&self) -> u64 {
        self.seq
    }

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
        action
    }

    fn update(&mut self, action: &Self::AuthorityAction) {
        self.seq += 1;
        self.sum += action;
    }
}

impl MessageEncoding for CounterState {
    fn write_to<T: io::Write>(&self, out: &mut T) -> io::Result<usize> {
        Ok(self.seq.write_to(out)? + self.sum.write_to(out)?)
    }

    fn read_from<T: io::Read>(read: &mut T) -> io::Result<Self> {
        Ok(Self {
            seq: MessageEncoding::read_from(read)?,
            sum: MessageEncoding::read_from(read)?,
        })
    }
}

#[derive(Clone)]
struct LocalhostTcpIo {
    address: u16,
    listener: Arc<TcpListener>,
}

impl LocalhostTcpIo {
    async fn bind_ephemeral() -> io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        Ok(Self {
            address: listener.local_addr()?.port(),
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

fn test_settings() -> SharedStateSettings {
    SharedStateSettings {
        net: NetIoSettings {
            process_timeout: Duration::from_millis(100),
            message_timeout: Duration::from_millis(250),
        },
        sync_timing: sharedstate::cluster::state_sync::StateSyncTiming {
            retry_delay: Duration::from_millis(50),
            ..Default::default()
        },
        ..SharedStateSettings::default()
    }
}

fn start_node<I>(io: Arc<I>, my_address: I::Address, leader_address: I::Address) -> SharedState<I, CounterState>
where
    I: SyncIOListener,
    I::Address: Into<u64>,
{
    SharedState::start(SharedStateConfig {
        io,
        my_address,
        leader_address,
        available_peers: vec![leader_address],
        initial_state: RecoverableState::new(my_address.into(), CounterState::default()),
        settings: test_settings(),
    })
    .unwrap()
}

async fn start_sim_node(
    net: &SimulatedNet,
    address: u64,
    leader_address: u64,
) -> SharedState<SimulatedIo, CounterState> {
    start_node(net.start_io(address).await, address, leader_address)
}

async fn wait_until(mut condition: impl FnMut() -> bool, failure: &str) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if condition() {
            return;
        }
        assert!(Instant::now() < deadline, "{failure}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn assert_never_for(mut condition: impl FnMut() -> bool, duration: Duration, failure: &str) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        assert!(!condition(), "{failure}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn state_sum<I: SyncIOListener>(node: &SharedState<I, CounterState>) -> u64 {
    let mut handle = node.state_handle();
    handle.read_with(|state| state.state().sum)
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_tcp_node_stops_new_traffic_or_marks_follower_disconnected() {
    let leader_io = LocalhostTcpIo::bind_ephemeral().await.unwrap();
    let follower_io = LocalhostTcpIo::bind_ephemeral().await.unwrap();
    let leader_address = leader_io.address;
    let follower_address = follower_io.address;

    let leader = start_node(Arc::new(leader_io), leader_address, leader_address);
    let follower = start_node(Arc::new(follower_io), follower_address, leader_address);

    wait_until(|| follower.is_connected_to_leader(), "follower never connected before leader drop").await;

    drop(leader);

    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let new_traffic_fails =
            tokio::time::timeout(Duration::from_millis(100), TcpStream::connect(("127.0.0.1", leader_address)))
                .await
                .map_or(true, |result| result.is_err());

        if new_traffic_fails || !follower.is_connected_to_leader() {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "dropped leader still accepted new TCP traffic and follower still reported connected"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_leader_no_longer_propagates_follower_actions() {
    let net = SimulatedNet::new();
    let leader = start_sim_node(&net, 1, 1).await;
    let follower = start_sim_node(&net, 2, 1).await;

    wait_until(|| follower.is_connected_to_leader(), "follower never connected before leader drop").await;

    follower.submit_action(5).await.unwrap();
    wait_until(
        || state_sum(&leader) == 5 && state_sum(&follower) == 5,
        "baseline action did not propagate before leader drop",
    )
    .await;

    let mut leader_state = leader.state_handle();
    drop(leader);

    follower.submit_action(7).await.unwrap();

    assert_never_for(
        || {
            let leader_sum = leader_state.read_with(|state| state.state().sum);
            leader_sum != 5 || state_sum(&follower) != 5
        },
        Duration::from_millis(500),
        "action propagated after the leader handle was dropped",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_node_closes_its_action_queue() {
    let net = SimulatedNet::new();
    let leader = start_sim_node(&net, 1, 1).await;
    let follower = start_sim_node(&net, 2, 1).await;

    wait_until(|| follower.is_connected_to_leader(), "follower never connected before queue close check").await;

    let leader_sender = leader.actions_sender();
    let follower_sender = follower.actions_sender();
    let mut leader_state = leader.state_handle();

    drop(follower);
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if follower_sender.send(11).await.is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            leader_state.read_with(|state| state.state().sum),
            0,
            "dropped follower propagated an action accepted during shutdown"
        );
        assert!(Instant::now() < deadline, "dropped follower action queue kept accepting work");
    }

    drop(leader);
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if leader_sender.send(13).await.is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            leader_state.read_with(|state| state.state().sum),
            0,
            "dropped leader applied an action accepted during shutdown"
        );
        assert!(Instant::now() < deadline, "dropped leader action queue kept accepting work");
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        leader_state.read_with(|state| state.state().sum),
        0,
        "dropped node action senders mutated retained state"
    );
}
