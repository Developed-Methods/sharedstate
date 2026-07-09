use std::{
    collections::BTreeMap,
    future::pending,
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use message_encoding::MessageEncoding;
use sharedstate::{
    protocol::{
        framing::{
            read_message_opt, read_message_to_vec, send_message, MessageSizeHeader, ReadMessageError, ReadMessageResult,
        },
        messages::{SyncRequest, SyncResponse, PROTOCOL_VERSION},
    },
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction},
    },
    transport::{
        channels::NetIoSettings,
        traits::{SyncConnection, SyncIO, SyncIOListener},
    },
    SharedState, SharedStateConfig, SharedStateSettings,
};
use tokio::{
    io::{duplex, split, AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
};

const PROCESS_TIMEOUT: Duration = Duration::from_millis(75);
const MESSAGE_TIMEOUT: Duration = Duration::from_millis(150);
const ASSERT_TIMEOUT: Duration = Duration::from_secs(3);

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
struct TestState {
    seq: u64,
    values: BTreeMap<String, String>,
}

impl DeterministicState for TestState {
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

impl MessageEncoding for TestState {
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

fn test_settings() -> SharedStateSettings {
    SharedStateSettings {
        net: NetIoSettings {
            process_timeout: PROCESS_TIMEOUT,
            message_timeout: MESSAGE_TIMEOUT,
        },
        sync_timing: sharedstate::cluster::state_sync::StateSyncTiming {
            retry_delay: Duration::from_millis(50),
            ..Default::default()
        },
        ..SharedStateSettings::default()
    }
}

async fn start_tcp_leader() -> (SharedState<LocalhostTcpIo, TestState>, u16) {
    let io = LocalhostTcpIo::bind_ephemeral().await.unwrap();
    let address = io.address;
    let node = SharedState::start(SharedStateConfig {
        io: Arc::new(io),
        my_address: address,
        leader_address: address,
        available_peers: vec![address],
        initial_state: RecoverableState::new(address as u64, TestState::default()),
        settings: test_settings(),
    })
    .unwrap();

    wait_until(|| node.is_leader(), "node never became leader").await;
    (node, address)
}

async fn read_handshake_rejection(stream: &mut TcpStream) -> Option<SyncResponse<TestState>> {
    let mut buffer = Vec::new();
    match read_message_opt::<SyncResponse<TestState>, _>(&mut buffer, stream, PROCESS_TIMEOUT, Some(ASSERT_TIMEOUT))
        .await
    {
        Ok(ReadMessageResult::Message(response)) => Some(response),
        Ok(ReadMessageResult::Close) => None,
        Ok(ReadMessageResult::KeepAlive) => panic!("server sent keepalive instead of rejecting handshake"),
        Err(error) if error.is_disconnect() => None,
        Err(error) => panic!("handshake rejection did not close cleanly: {error:?}"),
    }
}

async fn assert_rejected(response: Option<SyncResponse<TestState>>) {
    if let Some(response) = response {
        assert!(
            matches!(response, SyncResponse::NotConnected),
            "expected NotConnected or clean close, got {}",
            response.name(),
        );
    }
}

async fn read_response(stream: &mut TcpStream) -> SyncResponse<TestState> {
    let mut buffer = Vec::new();
    match read_message_opt::<SyncResponse<TestState>, _>(&mut buffer, stream, PROCESS_TIMEOUT, Some(ASSERT_TIMEOUT))
        .await
        .unwrap()
    {
        ReadMessageResult::Message(response) => response,
        ReadMessageResult::Close => panic!("server closed before response"),
        ReadMessageResult::KeepAlive => panic!("server sent keepalive instead of response"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn handshake_rejects_wrong_protocol_version() {
    let (_node, address) = start_tcp_leader().await;
    let mut stream = TcpStream::connect(("127.0.0.1", address)).await.unwrap();
    let mut buffer = Vec::new();

    send_message(
        &mut buffer,
        &SyncRequest::<TestState>::ProtocolVersion(PROTOCOL_VERSION + 1),
        &mut stream,
        PROCESS_TIMEOUT,
    )
    .await
    .unwrap();

    assert_rejected(read_handshake_rejection(&mut stream).await).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ping_rpc_returns_pong_after_handshake() {
    let (_node, address) = start_tcp_leader().await;
    let mut stream = TcpStream::connect(("127.0.0.1", address)).await.unwrap();
    let mut buffer = Vec::new();

    send_message(
        &mut buffer,
        &SyncRequest::<TestState>::ProtocolVersion(PROTOCOL_VERSION),
        &mut stream,
        PROCESS_TIMEOUT,
    )
    .await
    .unwrap();
    assert!(matches!(read_response(&mut stream).await, SyncResponse::Ok));

    send_message(&mut buffer, &SyncRequest::<TestState>::Ping(42), &mut stream, PROCESS_TIMEOUT)
        .await
        .unwrap();
    assert!(matches!(read_response(&mut stream).await, SyncResponse::Pong(42)));
}

#[tokio::test(flavor = "multi_thread")]
async fn handshake_rejects_missing_protocol_message() {
    let (_node, address) = start_tcp_leader().await;
    let mut stream = TcpStream::connect(("127.0.0.1", address)).await.unwrap();

    assert_rejected(read_handshake_rejection(&mut stream).await).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn handshake_rejects_unexpected_initial_request() {
    let (_node, address) = start_tcp_leader().await;
    let mut stream = TcpStream::connect(("127.0.0.1", address)).await.unwrap();
    let mut buffer = Vec::new();

    send_message(
        &mut buffer,
        &SyncRequest::<TestState>::Action(("before-handshake".to_owned(), "bad".to_owned())),
        &mut stream,
        PROCESS_TIMEOUT,
    )
    .await
    .unwrap();

    assert_rejected(read_handshake_rejection(&mut stream).await).await;
}

#[derive(Clone)]
struct OutOfSequenceLeaderIo {
    follower: u64,
    leader: u64,
    connects: Arc<AtomicUsize>,
}

impl SyncIO for OutOfSequenceLeaderIo {
    type Address = u64;
    type Read = ReadHalf<DuplexStream>;
    type Write = WriteHalf<DuplexStream>;

    async fn connect(&self, remote: &Self::Address) -> io::Result<SyncConnection<Self>> {
        assert_eq!(*remote, self.leader);
        self.connects.fetch_add(1, Ordering::SeqCst);

        let (client, server) = duplex(64 * 1024);
        let (client_read, client_write) = split(client);
        let (server_read, server_write) = split(server);
        let settings = NetIoSettings {
            process_timeout: PROCESS_TIMEOUT,
            message_timeout: MESSAGE_TIMEOUT,
        };

        tokio::spawn(serve_out_of_sequence_subscription(self.follower, server_read, server_write, settings));

        Ok(SyncConnection {
            remote: *remote,
            read: client_read,
            write: client_write,
        })
    }
}

impl SyncIOListener for OutOfSequenceLeaderIo {
    async fn next_client(&self) -> io::Result<SyncConnection<Self>> {
        pending().await
    }
}

async fn serve_out_of_sequence_subscription(
    remote: u64,
    read: ReadHalf<DuplexStream>,
    write: WriteHalf<DuplexStream>,
    settings: NetIoSettings,
) {
    let connection = SyncConnection::<OutOfSequenceLeaderIo> { remote, read, write };
    let (_remote, responses, mut requests) = connection.server_channels::<TestState>(settings);

    let Some(SyncRequest::ProtocolVersion(PROTOCOL_VERSION)) = requests.recv().await else {
        return;
    };
    if responses.send(SyncResponse::Ok).await.is_err() {
        return;
    }

    let Some(SyncRequest::Subscribe(_details)) = requests.recv().await else {
        return;
    };
    if responses.send(SyncResponse::Ok).await.is_err() {
        return;
    }

    let _ = responses
        .send(SyncResponse::Action {
            seq: 999,
            action: RecoverableStateAction::StateAction {
                action: ("out-of-sequence".to_owned(), "must-not-apply".to_owned()),
            },
        })
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_retries_and_does_not_apply_out_of_sequence_leader_stream() {
    let connects = Arc::new(AtomicUsize::new(0));
    let io = Arc::new(OutOfSequenceLeaderIo {
        follower: 10,
        leader: 20,
        connects: connects.clone(),
    });
    let follower = SharedState::start(SharedStateConfig {
        io,
        my_address: 10,
        leader_address: 20,
        available_peers: vec![20],
        initial_state: RecoverableState::new(10, TestState::default()),
        settings: test_settings(),
    })
    .unwrap();

    wait_until(|| connects.load(Ordering::SeqCst) >= 2, "follower did not retry after out-of-sequence stream").await;

    let mut handle = follower.state_handle();
    assert!(
        !handle.read_with(|state| state.state().values.contains_key("out-of-sequence")),
        "follower applied an action from an out-of-sequence leader stream",
    );
}

#[tokio::test]
async fn framing_rejects_partial_header_with_timeout() {
    let (mut writer, mut reader) = duplex(64);
    writer.write_all(&[0, 0]).await.unwrap();

    let mut buffer = Vec::new();
    let error = read_message_to_vec(&mut buffer, &mut reader, PROCESS_TIMEOUT, Some(Duration::from_millis(25)))
        .await
        .unwrap_err();

    assert!(matches!(error, ReadMessageError::NextMessageTimeout(_)));
}

#[tokio::test]
async fn framing_rejects_partial_payload_with_timeout() {
    let (mut writer, mut reader) = duplex(64);
    writer.write_all(&(8 as MessageSizeHeader).to_be_bytes()).await.unwrap();
    writer.write_all(&[1, 2]).await.unwrap();

    let mut buffer = Vec::new();
    let error = read_message_to_vec(&mut buffer, &mut reader, Duration::from_millis(25), Some(PROCESS_TIMEOUT))
        .await
        .unwrap_err();

    assert!(matches!(error, ReadMessageError::MessageReadTimeout));
}

#[tokio::test]
async fn framing_rejects_invalid_encoded_body() {
    let (mut writer, mut reader) = duplex(64);
    writer.write_all(&(2 as MessageSizeHeader).to_be_bytes()).await.unwrap();
    writer.write_all(&999u16.to_be_bytes()).await.unwrap();

    let mut buffer = Vec::new();
    let error =
        read_message_opt::<SyncRequest<TestState>, _>(&mut buffer, &mut reader, PROCESS_TIMEOUT, Some(PROCESS_TIMEOUT))
            .await
            .unwrap_err();

    assert!(matches!(error, ReadMessageError::EncodingError(_)));
}

#[tokio::test]
async fn framing_large_declared_length_times_out_without_accepting_message() {
    let (mut writer, mut reader) = duplex(64);
    writer.write_all(&(1024 * 1024_u32).to_be_bytes()).await.unwrap();

    let mut buffer = Vec::new();
    let error = read_message_to_vec(&mut buffer, &mut reader, Duration::from_millis(25), Some(PROCESS_TIMEOUT))
        .await
        .unwrap_err();

    assert!(matches!(error, ReadMessageError::MessageReadTimeout));
    assert_eq!(buffer.len(), 1024 * 1024);
}

async fn wait_until(mut condition: impl FnMut() -> bool, failure: &'static str) {
    let deadline = Instant::now() + ASSERT_TIMEOUT;
    loop {
        if condition() {
            return;
        }
        assert!(Instant::now() < deadline, "{failure}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
