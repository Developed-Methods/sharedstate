use std::{
    collections::{hash_map, HashMap},
    sync::Arc,
    time::Duration,
};

use message_encoding::MessageEncoding;
use tokio::sync::{
    mpsc::{error::TrySendError, Receiver, Sender},
    oneshot, Mutex,
};

use crate::{
    new::node_state::NodeState,
    protocol::messages::{SyncRequest, SyncResponse, PROTOCOL_VERSION},
    state::determinstic_state::DeterministicState,
    transport::{
        channels::NetIoSettings,
        traits::{SyncIO, SyncIOAddress},
    },
};

const RPC_QUEUE_CAPACITY: usize = 512;
const CONNECT_RETRY_LIMIT: u64 = 3;
const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(100);

pub struct PeerConnections<I: SyncIO, D: DeterministicState> {
    io: Arc<I>,
    conn_settings: NetIoSettings,
    state: Arc<NodeState<I::Address, D>>,
    connections: Mutex<HashMap<I::Address, Connection<I::Address, D>>>,
}

struct Connection<A: SyncIOAddress, D: DeterministicState> {
    tx: Sender<RpcMessage<A, D>>,
}

struct RpcMessage<A: SyncIOAddress, D: DeterministicState> {
    request: SyncRequest<A, D>,
    response: oneshot::Sender<Result<SyncResponse<A, D>, PeerRpcError>>,
}

type RpcResponseSender<A, D> = oneshot::Sender<Result<SyncResponse<A, D>, PeerRpcError>>;

impl<I, D> PeerConnections<I, D>
where
    I: SyncIO,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    pub fn new(io: Arc<I>, conn_settings: NetIoSettings, state: Arc<NodeState<I::Address, D>>) -> Self {
        Self {
            io,
            conn_settings,
            state,
            connections: Mutex::new(HashMap::new()),
        }
    }

    pub async fn send_rpc(
        &self,
        peer: I::Address,
        request: SyncRequest<I::Address, D>,
    ) -> Result<SyncResponse<I::Address, D>, PeerRpcError> {
        if matches!(&request, SyncRequest::SubscribeFresh | SyncRequest::SubscribeRecovery(_)) {
            return Err(PeerRpcError::RequestNotAllowedOverRpc);
        }

        let (tx, rx) = oneshot::channel();
        let mut pending = Some(RpcMessage { request, response: tx });

        loop {
            let msg = pending.take().expect("rpc message missing while send is still pending");
            let mut connections = self.connections.lock().await;

            match connections.entry(peer) {
                hash_map::Entry::Occupied(entry) => match entry.get().tx.try_send(msg) {
                    Ok(()) => {
                        drop(connections);
                        return await_rpc_response(rx).await;
                    }
                    Err(TrySendError::Closed(msg)) => {
                        entry.remove();
                        pending = Some(msg);
                    }
                    Err(TrySendError::Full(msg)) => {
                        let sender = entry.get().tx.clone();
                        drop(connections);

                        match sender.send(msg).await {
                            Ok(()) => return await_rpc_response(rx).await,
                            Err(error) => {
                                pending = Some(error.0);
                                let mut connections = self.connections.lock().await;
                                if connections.get(&peer).is_some_and(|conn| conn.tx.is_closed()) {
                                    connections.remove(&peer);
                                }
                            }
                        }
                    }
                },
                hash_map::Entry::Vacant(entry) => {
                    let conn =
                        Connection::create(peer, self.state.my_address, self.io.clone(), self.conn_settings.clone());
                    entry.insert(conn);
                    pending = Some(msg);
                }
            }
        }
    }
}

async fn await_rpc_response<A: SyncIOAddress, D: DeterministicState>(
    rx: oneshot::Receiver<Result<SyncResponse<A, D>, PeerRpcError>>,
) -> Result<SyncResponse<A, D>, PeerRpcError> {
    match rx.await {
        Ok(res) => res,
        Err(_) => Err(PeerRpcError::ResponseDropped),
    }
}

impl<A, D> Connection<A, D>
where
    A: SyncIOAddress,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    pub fn create<I>(remote_addr: A, local_addr: A, io: Arc<I>, settings: NetIoSettings) -> Self
    where
        I: SyncIO<Address = A>,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(RPC_QUEUE_CAPACITY);

        tokio::spawn(
            ConnectionWorker {
                rx,
                remote_addr,
                local_addr,
            }
            .run(io, settings),
        );

        Self { tx }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerRpcError {
    RequestNotAllowedOverRpc,
    FailedToConnectToPeer,
    FailedToSendRequest,
    FailedToReceiveResponse,
    HandshakeRejected,
    ResponseTimedOut,
    ResponseDropped,
}

struct ConnectionWorker<A: SyncIOAddress, D: DeterministicState> {
    rx: Receiver<RpcMessage<A, D>>,
    remote_addr: A,
    local_addr: A,
}

impl<A: SyncIOAddress, D: DeterministicState> ConnectionWorker<A, D>
where
    D: MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    async fn run<I: SyncIO<Address = A>>(mut self, io: Arc<I>, settings: NetIoSettings) {
        let mut repeat_failures = 0;

        let connection = loop {
            match io.connect(&self.remote_addr).await {
                Ok(connection) => break connection,
                Err(error) => {
                    tracing::error!(?error, repeat_failures, "failed to connect to peer {:?}", self.remote_addr);
                    repeat_failures += 1;

                    if repeat_failures > CONNECT_RETRY_LIMIT {
                        self.drain_with_error(PeerRpcError::FailedToConnectToPeer).await;
                        return;
                    }

                    tokio::time::sleep(CONNECT_RETRY_DELAY).await;
                }
            }
        };

        let (_remote, write, mut read) = connection.client_channels::<D>(settings.clone());

        if let Err(error) = self.handshake(&write, &mut read, settings.message_timeout).await {
            self.drain_with_error(error).await;
            return;
        }

        let (response_tx, response_rx) = tokio::sync::mpsc::channel(RPC_QUEUE_CAPACITY);
        tokio::spawn(
            ResponseReader::<A, D> {
                responses: read,
                response_senders: response_rx,
                timeout: settings.message_timeout,
            }
            .run(),
        );

        while let Some(msg) = self.rx.recv().await {
            if write.send(msg.request).await.is_err() {
                let _ = msg.response.send(Err(PeerRpcError::FailedToSendRequest));
                self.drain_with_error(PeerRpcError::FailedToSendRequest).await;
                return;
            }

            if let Err(error) = response_tx.send(msg.response).await {
                let _ = error.0.send(Err(PeerRpcError::FailedToReceiveResponse));
                self.drain_with_error(PeerRpcError::FailedToReceiveResponse).await;
                return;
            }
        }
    }

    async fn handshake(
        &self,
        write: &Sender<SyncRequest<A, D>>,
        read: &mut Receiver<SyncResponse<A, D>>,
        timeout: Duration,
    ) -> Result<(), PeerRpcError> {
        write
            .send(SyncRequest::ProtocolVersion(PROTOCOL_VERSION))
            .await
            .map_err(|_| PeerRpcError::FailedToSendRequest)?;
        require_ok(read_response(read, timeout).await?)?;

        write
            .send(SyncRequest::MyAddress(self.local_addr))
            .await
            .map_err(|_| PeerRpcError::FailedToSendRequest)?;
        require_ok(read_response(read, timeout).await?)?;

        Ok(())
    }

    async fn drain_with_error(&mut self, error: PeerRpcError) {
        self.rx.close();
        while let Some(msg) = self.rx.recv().await {
            let _ = msg.response.send(Err(error.clone()));
        }
    }
}

struct ResponseReader<A: SyncIOAddress, D: DeterministicState> {
    responses: Receiver<SyncResponse<A, D>>,
    response_senders: Receiver<RpcResponseSender<A, D>>,
    timeout: Duration,
}

impl<A: SyncIOAddress, D: DeterministicState> ResponseReader<A, D> {
    async fn run(mut self) {
        while let Some(response_sender) = self.response_senders.recv().await {
            let response = read_response(&mut self.responses, self.timeout).await;
            let fatal_error = response.as_ref().err().cloned();
            let _ = response_sender.send(response);

            if let Some(error) = fatal_error {
                self.drain_with_error(error).await;
                return;
            }
        }
    }

    async fn drain_with_error(&mut self, error: PeerRpcError) {
        self.response_senders.close();
        while let Some(response_sender) = self.response_senders.recv().await {
            let _ = response_sender.send(Err(error.clone()));
        }
    }
}

async fn read_response<A: SyncIOAddress, D: DeterministicState>(
    read: &mut Receiver<SyncResponse<A, D>>,
    timeout: Duration,
) -> Result<SyncResponse<A, D>, PeerRpcError> {
    match tokio::time::timeout(timeout, read.recv()).await {
        Ok(Some(response)) => Ok(response),
        Ok(None) => Err(PeerRpcError::FailedToReceiveResponse),
        Err(_) => Err(PeerRpcError::ResponseTimedOut),
    }
}

fn require_ok<A: SyncIOAddress, D: DeterministicState>(response: SyncResponse<A, D>) -> Result<(), PeerRpcError> {
    match response {
        SyncResponse::Ok => Ok(()),
        _ => Err(PeerRpcError::HandshakeRejected),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io::{Error, ErrorKind, Result},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use sequenced_broadcast::SequencedBroadcastSettings;
    use tokio::{
        io::{duplex, split, DuplexStream, ReadHalf, WriteHalf},
        sync::mpsc,
    };

    use super::*;
    use crate::{
        new::{
            node_state::NodeState, subscribable_state::SubscribableState, tasks::current_leader::CurrentLeaderStatus,
        },
        protocol::messages::PROTOCOL_VERSION,
        state::recoverable_state::RecoverableState,
        transport::traits::SyncConnection,
    };

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestState(u64);

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestAction(u64);

    impl DeterministicState for TestState {
        type Action = TestAction;
        type AuthorityAction = TestAction;

        fn accept_seq(&self) -> u64 {
            self.0
        }

        fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
            action
        }

        fn update(&mut self, _action: &Self::AuthorityAction) {
            self.0 += 1;
        }
    }

    impl MessageEncoding for TestState {
        fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
            self.0.write_to(out)
        }

        fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
            Ok(Self(MessageEncoding::read_from(read)?))
        }
    }

    impl MessageEncoding for TestAction {
        fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
            self.0.write_to(out)
        }

        fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
            Ok(Self(MessageEncoding::read_from(read)?))
        }
    }

    struct FakeIo {
        address: u64,
        connect_count: Arc<AtomicUsize>,
        incoming: mpsc::UnboundedSender<SyncConnection<FakeIo>>,
        fail_connect: bool,
    }

    impl SyncIO for FakeIo {
        type Address = u64;
        type Read = ReadHalf<DuplexStream>;
        type Write = WriteHalf<DuplexStream>;

        async fn connect(&self, remote: &Self::Address) -> Result<SyncConnection<Self>> {
            self.connect_count.fetch_add(1, Ordering::SeqCst);

            if self.fail_connect {
                return Err(Error::new(ErrorKind::ConnectionRefused, "test connect failure"));
            }

            let (client, server) = duplex(4096);
            let (client_read, client_write) = split(client);
            let (server_read, server_write) = split(server);

            self.incoming
                .send(SyncConnection {
                    remote: self.address,
                    read: server_read,
                    write: server_write,
                })
                .map_err(|_| Error::new(ErrorKind::BrokenPipe, "test server dropped"))?;

            Ok(SyncConnection {
                remote: *remote,
                read: client_read,
                write: client_write,
            })
        }
    }

    fn settings() -> NetIoSettings {
        NetIoSettings {
            process_timeout: Duration::from_millis(100),
            message_timeout: Duration::from_millis(500),
        }
    }

    fn node_state(address: u64) -> Arc<NodeState<u64, TestState>> {
        Arc::new(NodeState {
            my_address: address,
            can_lead: true,
            peers: Mutex::new(HashMap::new()),
            state: SubscribableState::new(
                RecoverableState::new(address, TestState(1)),
                SequencedBroadcastSettings::default(),
            )
            .unwrap(),
            leader_status: Arc::new(CurrentLeaderStatus::new(address)),
        })
    }

    fn peer_connections(
        local_address: u64,
        fail_connect: bool,
    ) -> (Arc<PeerConnections<FakeIo, TestState>>, mpsc::UnboundedReceiver<SyncConnection<FakeIo>>, Arc<AtomicUsize>)
    {
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let connect_count = Arc::new(AtomicUsize::new(0));
        let io = Arc::new(FakeIo {
            address: local_address,
            connect_count: connect_count.clone(),
            incoming: incoming_tx,
            fail_connect,
        });

        (Arc::new(PeerConnections::new(io, settings(), node_state(local_address))), incoming_rx, connect_count)
    }

    async fn accept_server(
        incoming: &mut mpsc::UnboundedReceiver<SyncConnection<FakeIo>>,
    ) -> (Sender<SyncResponse<u64, TestState>>, Receiver<SyncRequest<u64, TestState>>) {
        let conn = tokio::time::timeout(Duration::from_secs(1), incoming.recv())
            .await
            .unwrap()
            .unwrap();
        let (_remote, write, read) = conn.server_channels::<TestState>(settings());
        (write, read)
    }

    async fn recv_request(read: &mut Receiver<SyncRequest<u64, TestState>>) -> SyncRequest<u64, TestState> {
        tokio::time::timeout(Duration::from_secs(1), read.recv())
            .await
            .unwrap()
            .unwrap()
    }

    async fn expect_handshake(
        write: &Sender<SyncResponse<u64, TestState>>,
        read: &mut Receiver<SyncRequest<u64, TestState>>,
        expected_local: u64,
    ) {
        match recv_request(read).await {
            SyncRequest::ProtocolVersion(version) => assert_eq!(version, PROTOCOL_VERSION),
            other => panic!("expected protocol version request, got {other:?}"),
        }
        write.send(SyncResponse::Ok).await.unwrap();

        match recv_request(read).await {
            SyncRequest::MyAddress(address) => assert_eq!(address, expected_local),
            other => panic!("expected my-address request, got {other:?}"),
        }
        write.send(SyncResponse::Ok).await.unwrap();
    }

    #[tokio::test]
    async fn rejects_subscribe_recovery_rpc_without_connecting() {
        let (connections, _incoming, connect_count) = peer_connections(1, false);

        let result = connections
            .send_rpc(
                2,
                SyncRequest::SubscribeRecovery(crate::state::recoverable_state::RecoverableStateDetails::new(1, 1)),
            )
            .await;

        assert!(matches!(result, Err(PeerRpcError::RequestNotAllowedOverRpc)));
        assert_eq!(connect_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn rejects_subscribe_fresh_rpc_without_connecting() {
        let (connections, _incoming, connect_count) = peer_connections(1, false);

        let result = connections.send_rpc(2, SyncRequest::SubscribeFresh).await;

        assert!(matches!(result, Err(PeerRpcError::RequestNotAllowedOverRpc)));
        assert_eq!(connect_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn handshakes_before_first_rpc() {
        let (connections, mut incoming, _connect_count) = peer_connections(1, false);
        let task = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(42)).await }
        });

        let (write, mut read) = accept_server(&mut incoming).await;
        expect_handshake(&write, &mut read, 1).await;

        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => assert_eq!(id, 42),
            other => panic!("expected ping request, got {other:?}"),
        }
        write.send(SyncResponse::Pong(42)).await.unwrap();

        assert!(matches!(task.await.unwrap().unwrap(), SyncResponse::Pong(42)));
    }

    #[tokio::test]
    async fn reuses_connection_for_multiple_rpcs() {
        let (connections, mut incoming, connect_count) = peer_connections(1, false);

        let first = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(1)).await }
        });
        let (write, mut read) = accept_server(&mut incoming).await;
        expect_handshake(&write, &mut read, 1).await;

        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => assert_eq!(id, 1),
            other => panic!("expected first ping request, got {other:?}"),
        }
        write.send(SyncResponse::Pong(1)).await.unwrap();
        assert!(matches!(first.await.unwrap().unwrap(), SyncResponse::Pong(1)));

        let second = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(2)).await }
        });

        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => assert_eq!(id, 2),
            other => panic!("expected second ping request, got {other:?}"),
        }
        write.send(SyncResponse::Pong(2)).await.unwrap();
        assert!(matches!(second.await.unwrap().unwrap(), SyncResponse::Pong(2)));
        assert_eq!(connect_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pipelines_requests_before_responses_arrive() {
        let (connections, mut incoming, connect_count) = peer_connections(1, false);

        let first = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(1)).await }
        });
        let second = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(2)).await }
        });

        let (write, mut read) = accept_server(&mut incoming).await;
        expect_handshake(&write, &mut read, 1).await;

        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => assert_eq!(id, 1),
            other => panic!("expected first pipelined ping request, got {other:?}"),
        }
        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => assert_eq!(id, 2),
            other => panic!("expected second pipelined ping request, got {other:?}"),
        }

        write.send(SyncResponse::Pong(1)).await.unwrap();
        write.send(SyncResponse::Pong(2)).await.unwrap();

        assert!(matches!(first.await.unwrap().unwrap(), SyncResponse::Pong(1)));
        assert!(matches!(second.await.unwrap().unwrap(), SyncResponse::Pong(2)));
        assert_eq!(connect_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn connect_failure_propagates_to_queued_rpc() {
        let (connections, _incoming, _connect_count) = peer_connections(1, true);

        let result = tokio::time::timeout(Duration::from_secs(2), connections.send_rpc(2, SyncRequest::Ping(1)))
            .await
            .unwrap();

        assert!(matches!(result, Err(PeerRpcError::FailedToConnectToPeer)));
    }
}
