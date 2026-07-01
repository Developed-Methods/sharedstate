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
use tokio_util::sync::CancellationToken;

use crate::{
    new::node_state::NodeState,
    protocol::messages::{LeaderWithElectionInfo, SharePeerDetails, SyncRequest, SyncResponse, PROTOCOL_VERSION},
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
    cancel: CancellationToken,
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
                hash_map::Entry::Occupied(entry) => {
                    if entry.get().cancel.is_cancelled() {
                        let connection = entry.remove();
                        drop(connections);
                        connection.kill().await;
                        pending = Some(msg);
                    } else {
                        match entry.get().tx.try_send(msg) {
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
                                        if connections
                                            .get(&peer)
                                            .is_some_and(|conn| conn.tx.is_closed() || conn.cancel.is_cancelled())
                                        {
                                            connections.remove(&peer);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                hash_map::Entry::Vacant(entry) => {
                    let conn = Connection::create(
                        peer,
                        self.state.my_address,
                        self.state.clone(),
                        self.io.clone(),
                        self.conn_settings.clone(),
                    );
                    entry.insert(conn);
                    pending = Some(msg);
                }
            }
        }
    }

    pub async fn kill_connection(&self, peer: I::Address) {
        let connection = self.connections.lock().await.remove(&peer);
        if let Some(connection) = connection {
            connection.kill().await;
        }
    }

    pub async fn send_ping(&self, peer: I::Address, id: u64) -> Result<(), PeerRpcError> {
        let response = self.send_rpc(peer, SyncRequest::Ping(id)).await?;
        match response {
            SyncResponse::Pong(response_id) if response_id == id => Ok(()),
            response => self.unexpected_response(peer, "Pong with matching id", response).await,
        }
    }

    pub async fn send_peers_info(
        &self,
        peer: I::Address,
        peers: Vec<SharePeerDetails<I::Address>>,
    ) -> Result<Vec<SharePeerDetails<I::Address>>, PeerRpcError> {
        let response = self.send_rpc(peer, SyncRequest::SharePeers(peers)).await?;
        match response {
            SyncResponse::Peers(peers) => Ok(peers),
            response => self.unexpected_response(peer, "Peers", response).await,
        }
    }

    pub async fn send_leader_info(
        &self,
        peer: I::Address,
        source: I::Address,
        info: LeaderWithElectionInfo<I::Address>,
    ) -> Result<(), PeerRpcError> {
        let response = self
            .send_rpc(peer, SyncRequest::LeaderInformation { source, info })
            .await?;
        match response {
            SyncResponse::Ok => Ok(()),
            response => self.unexpected_response(peer, "Ok", response).await,
        }
    }

    pub async fn request_leader_info(
        &self,
        peer: I::Address,
    ) -> Result<LeaderWithElectionInfo<I::Address>, PeerRpcError> {
        let response = self.send_rpc(peer, SyncRequest::ShareLeaderInfo).await?;
        match response {
            SyncResponse::LeaderInfo(info) if info.observer == peer => Ok(info),
            response => {
                self.unexpected_response(peer, "LeaderInfo matching peer", response)
                    .await
            }
        }
    }

    async fn unexpected_response<T>(
        &self,
        peer: I::Address,
        expected: &'static str,
        response: SyncResponse<I::Address, D>,
    ) -> Result<T, PeerRpcError> {
        let actual = response.name();
        tracing::debug!(?peer, expected, actual, "peer returned unexpected rpc response");
        self.kill_connection(peer).await;
        Err(PeerRpcError::UnexpectedResponse { expected, actual })
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
    pub fn create<I>(
        remote_addr: A,
        local_addr: A,
        state: Arc<NodeState<A, D>>,
        io: Arc<I>,
        settings: NetIoSettings,
    ) -> Self
    where
        I: SyncIO<Address = A>,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(RPC_QUEUE_CAPACITY);
        let cancel = CancellationToken::new();

        tokio::spawn(
            ConnectionWorker {
                rx,
                cancel: cancel.clone(),
                remote_addr,
                local_addr,
                state,
            }
            .run(io, settings),
        );

        Self { tx, cancel }
    }

    async fn kill(self) {
        self.cancel.cancel();
        self.tx.closed().await;
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
    UnexpectedResponse {
        expected: &'static str,
        actual: &'static str,
    },
}

struct ConnectionWorker<A: SyncIOAddress, D: DeterministicState> {
    rx: Receiver<RpcMessage<A, D>>,
    cancel: CancellationToken,
    remote_addr: A,
    local_addr: A,
    state: Arc<NodeState<A, D>>,
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
                        self.state.set_peer_connected(self.remote_addr, false).await;
                        self.drain_with_error(PeerRpcError::FailedToConnectToPeer).await;
                        return;
                    }

                    tokio::time::sleep(CONNECT_RETRY_DELAY).await;
                }
            }
        };

        let (_remote, write, mut read) = connection.client_channels::<D>(settings.clone());

        if let Err(error) = self.handshake(&write, &mut read, settings.message_timeout).await {
            self.state.set_peer_connected(self.remote_addr, false).await;
            self.drain_with_error(error).await;
            return;
        }

        self.state.set_peer_connected(self.remote_addr, true).await;

        let (response_tx, response_rx) = tokio::sync::mpsc::channel(RPC_QUEUE_CAPACITY);
        tokio::spawn(
            ResponseReader::<A, D> {
                responses: read,
                response_senders: response_rx,
                timeout: settings.message_timeout,
                cancel: self.cancel.clone(),
            }
            .run(),
        );

        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    self.drain_with_error(PeerRpcError::FailedToReceiveResponse).await;
                    break;
                }
                msg = self.rx.recv() => {
                    let Some(msg) = msg else {
                        break;
                    };

                    if write.send(msg.request).await.is_err() {
                        let _ = msg.response.send(Err(PeerRpcError::FailedToSendRequest));
                        self.drain_with_error(PeerRpcError::FailedToSendRequest).await;
                        break;
                    }

                    if let Err(error) = response_tx.send(msg.response).await {
                        let _ = error.0.send(Err(PeerRpcError::FailedToReceiveResponse));
                        self.drain_with_error(PeerRpcError::FailedToReceiveResponse).await;
                        break;
                    }
                }
            }
        }

        self.state.set_peer_connected(self.remote_addr, false).await;
        self.cancel.cancel();
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
    cancel: CancellationToken,
}

impl<A: SyncIOAddress, D: DeterministicState> ResponseReader<A, D> {
    async fn run(mut self) {
        while let Some(response_sender) = self.response_senders.recv().await {
            let response = read_response(&mut self.responses, self.timeout).await;
            let fatal_error = response.as_ref().err().cloned();
            let _ = response_sender.send(response);

            if let Some(error) = fatal_error {
                self.cancel.cancel();
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
    use std::{collections::HashMap, io::Result, sync::Arc, time::Duration};

    use sequenced_broadcast::SequencedBroadcastSettings;

    use super::*;
    use crate::{
        new::{
            node_state::NodeState, subscribable_state::SubscribableState, tasks::current_leader::CurrentLeaderStatus,
        },
        protocol::messages::PROTOCOL_VERSION,
        state::recoverable_state::RecoverableState,
        transport::{
            simulated::{SimulatedIo, SimulatedNet},
            traits::SyncIOListener,
        },
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

    async fn peer_connections(
        local_address: u64,
        remote_address: u64,
        start_remote: bool,
    ) -> (Arc<PeerConnections<SimulatedIo, TestState>>, Option<Arc<SimulatedIo>>, Arc<NodeState<u64, TestState>>) {
        let net = SimulatedNet::new();
        let local_io = net.start_io(local_address).await;
        let remote_io = if start_remote {
            Some(net.start_io(remote_address).await)
        } else {
            None
        };
        let state = node_state(local_address);

        (Arc::new(PeerConnections::new(local_io, settings(), state.clone())), remote_io, state)
    }

    async fn accept_server(
        remote_io: &SimulatedIo,
    ) -> (Sender<SyncResponse<u64, TestState>>, Receiver<SyncRequest<u64, TestState>>) {
        let conn = tokio::time::timeout(Duration::from_secs(1), remote_io.next_client())
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
        let (connections, remote_io, _state) = peer_connections(1, 2, true).await;
        let remote_io = remote_io.unwrap();

        let result = connections
            .send_rpc(
                2,
                SyncRequest::SubscribeRecovery(crate::state::recoverable_state::RecoverableStateDetails::new(1, 1)),
            )
            .await;

        assert!(matches!(result, Err(PeerRpcError::RequestNotAllowedOverRpc)));
        assert!(tokio::time::timeout(Duration::from_millis(50), remote_io.next_client())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn rejects_subscribe_fresh_rpc_without_connecting() {
        let (connections, remote_io, _state) = peer_connections(1, 2, true).await;
        let remote_io = remote_io.unwrap();

        let result = connections.send_rpc(2, SyncRequest::SubscribeFresh).await;

        assert!(matches!(result, Err(PeerRpcError::RequestNotAllowedOverRpc)));
        assert!(tokio::time::timeout(Duration::from_millis(50), remote_io.next_client())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn handshakes_before_first_rpc() {
        let (connections, remote_io, _state) = peer_connections(1, 2, true).await;
        let remote_io = remote_io.unwrap();
        let task = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(42)).await }
        });

        let (write, mut read) = accept_server(&remote_io).await;
        expect_handshake(&write, &mut read, 1).await;

        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => assert_eq!(id, 42),
            other => panic!("expected ping request, got {other:?}"),
        }
        write.send(SyncResponse::Pong(42)).await.unwrap();

        assert!(matches!(task.await.unwrap().unwrap(), SyncResponse::Pong(42)));
    }

    #[tokio::test]
    async fn typed_rpc_unexpected_response_kills_connection() {
        let (connections, remote_io, _state) = peer_connections(1, 2, true).await;
        let remote_io = remote_io.unwrap();

        let first = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_ping(2, 1).await }
        });
        let (write, mut read) = accept_server(&remote_io).await;
        expect_handshake(&write, &mut read, 1).await;
        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => assert_eq!(id, 1),
            other => panic!("expected first typed ping request, got {other:?}"),
        }
        write.send(SyncResponse::Ok).await.unwrap();
        assert!(matches!(
            first.await.unwrap(),
            Err(PeerRpcError::UnexpectedResponse {
                expected: "Pong with matching id",
                actual: "Ok",
            })
        ));

        let second = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_ping(2, 2).await }
        });
        let (write, mut read) = accept_server(&remote_io).await;
        expect_handshake(&write, &mut read, 1).await;
        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => assert_eq!(id, 2),
            other => panic!("expected second typed ping request, got {other:?}"),
        }
        write.send(SyncResponse::Pong(2)).await.unwrap();
        assert!(second.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn reuses_connection_for_multiple_rpcs() {
        let (connections, remote_io, _state) = peer_connections(1, 2, true).await;
        let remote_io = remote_io.unwrap();

        let first = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(1)).await }
        });
        let (write, mut read) = accept_server(&remote_io).await;
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
        assert!(tokio::time::timeout(Duration::from_millis(50), remote_io.next_client())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn pipelines_requests_before_responses_arrive() {
        let (connections, remote_io, _state) = peer_connections(1, 2, true).await;
        let remote_io = remote_io.unwrap();

        let first = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(1)).await }
        });
        let second = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(2)).await }
        });

        let (write, mut read) = accept_server(&remote_io).await;
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
        assert!(tokio::time::timeout(Duration::from_millis(50), remote_io.next_client())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn connect_failure_propagates_to_queued_rpc() {
        let (connections, _remote_io, _state) = peer_connections(1, 2, false).await;

        let result = tokio::time::timeout(Duration::from_secs(2), connections.send_rpc(2, SyncRequest::Ping(1)))
            .await
            .unwrap();

        assert!(matches!(result, Err(PeerRpcError::FailedToConnectToPeer)));
    }

    #[tokio::test]
    async fn successful_connection_marks_peer_connected() {
        let (connections, remote_io, state) = peer_connections(1, 2, true).await;
        let remote_io = remote_io.unwrap();
        let task = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(42)).await }
        });

        let (write, mut read) = accept_server(&remote_io).await;
        expect_handshake(&write, &mut read, 1).await;

        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => assert_eq!(id, 42),
            other => panic!("expected ping request, got {other:?}"),
        }
        write.send(SyncResponse::Pong(42)).await.unwrap();
        assert!(matches!(task.await.unwrap().unwrap(), SyncResponse::Pong(42)));

        let peers = state.peers.lock().await;
        assert!(peers.get(&2).is_some_and(|peer| peer.is_connected));
    }

    #[tokio::test]
    async fn kill_connection_marks_disconnected_and_recreates_next_connection() {
        let (connections, remote_io, state) = peer_connections(1, 2, true).await;
        let remote_io = remote_io.unwrap();

        let first = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(1)).await }
        });
        let (write, mut read) = accept_server(&remote_io).await;
        expect_handshake(&write, &mut read, 1).await;
        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => assert_eq!(id, 1),
            other => panic!("expected first ping request, got {other:?}"),
        }
        write.send(SyncResponse::Pong(1)).await.unwrap();
        assert!(matches!(first.await.unwrap().unwrap(), SyncResponse::Pong(1)));

        connections.kill_connection(2).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if state.peers.lock().await.get(&2).is_some_and(|peer| !peer.is_connected) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let second = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(2)).await }
        });
        let (write, mut read) = accept_server(&remote_io).await;
        expect_handshake(&write, &mut read, 1).await;
        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => assert_eq!(id, 2),
            other => panic!("expected second ping request, got {other:?}"),
        }
        write.send(SyncResponse::Pong(2)).await.unwrap();
        assert!(matches!(second.await.unwrap().unwrap(), SyncResponse::Pong(2)));
    }

    #[tokio::test]
    async fn response_reader_failure_cancels_worker_and_next_rpc_reconnects() {
        let (connections, remote_io, state) = peer_connections(1, 2, true).await;
        let remote_io = remote_io.unwrap();

        let first = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(1)).await }
        });
        let (_write, mut read) = accept_server(&remote_io).await;
        expect_handshake(&_write, &mut read, 1).await;
        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => assert_eq!(id, 1),
            other => panic!("expected first ping request, got {other:?}"),
        }

        let result = tokio::time::timeout(Duration::from_secs(2), first)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(PeerRpcError::ResponseTimedOut)));

        let second = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(2)).await }
        });
        let (write, mut read) = accept_server(&remote_io).await;
        expect_handshake(&write, &mut read, 1).await;
        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => assert_eq!(id, 2),
            other => panic!("expected second ping request, got {other:?}"),
        }
        write.send(SyncResponse::Pong(2)).await.unwrap();
        assert!(matches!(second.await.unwrap().unwrap(), SyncResponse::Pong(2)));

        let peers = state.peers.lock().await;
        assert!(peers.get(&2).is_some_and(|peer| peer.is_connected));
    }

    #[tokio::test]
    async fn connect_failure_marks_peer_disconnected() {
        let (connections, _remote_io, state) = peer_connections(1, 2, false).await;

        let result = tokio::time::timeout(Duration::from_secs(2), connections.send_rpc(2, SyncRequest::Ping(1)))
            .await
            .unwrap();

        assert!(matches!(result, Err(PeerRpcError::FailedToConnectToPeer)));

        let peers = state.peers.lock().await;
        assert!(peers.get(&2).is_some_and(|peer| !peer.is_connected));
    }
}
