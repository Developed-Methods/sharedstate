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
    cluster::node_state::NodeState,
    protocol::messages::{LeaderInfo, LeaderState, SharePeerDetails, SyncRequest, SyncResponse, PROTOCOL_VERSION},
    state::deterministic_state::DeterministicState,
    transport::{
        channels::NetIoSettings,
        traits::{SyncIO, SyncIOAddress},
    },
};

const RPC_QUEUE_CAPACITY: usize = 512;
const CONNECT_RETRY_LIMIT: u64 = 3;
const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(100);
const CONNECT_FAIL_HOLD_DELAY: Duration = Duration::from_secs(5);

type ConnectionMap<I, D> = HashMap<<I as SyncIO>::Address, Connection<<I as SyncIO>::Address, D>>;

pub struct PeerConnections<I: SyncIO, D: DeterministicState> {
    io: Arc<I>,
    conn_settings: NetIoSettings,
    state: Arc<NodeState<I::Address, D>>,
    connections: Mutex<ConnectionMap<I, D>>,
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

    pub async fn send_leader_info(&self, peer: I::Address, info: LeaderInfo<I::Address>) -> Result<(), PeerRpcError> {
        let response = self.send_rpc(peer, SyncRequest::LeaderInformation(info)).await?;
        match response {
            SyncResponse::Ok => Ok(()),
            response => self.unexpected_response(peer, "Ok", response).await,
        }
    }

    pub async fn query_leader(&self, peer: I::Address) -> Result<LeaderState<I::Address>, PeerRpcError> {
        let response = self.send_rpc(peer, SyncRequest::LeaderQuery).await?;
        match response {
            SyncResponse::LeaderState(state) => Ok(state),
            response => self.unexpected_response(peer, "LeaderState", response).await,
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
        let mut retry_wait = CONNECT_RETRY_DELAY;

        let connection = loop {
            /* connect gives no timing guarantee; bound it so a hanging
             * transport surfaces as a normal connect failure */
            let connect_result = match tokio::time::timeout(settings.message_timeout, io.connect(&self.remote_addr)).await
            {
                Ok(result) => result,
                Err(_) => Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timed out connecting to peer",
                )),
            };

            match connect_result {
                Ok(connection) => break connection,
                Err(error) => {
                    tracing::error!(?error, repeat_failures, "failed to connect to peer {:?}", self.remote_addr);
                    repeat_failures += 1;

                    if repeat_failures > CONNECT_RETRY_LIMIT {
                        self.state.mark_peer_failed_to_connect(self.remote_addr).await;
                        self.drain_with_error(PeerRpcError::FailedToConnectToPeer).await;
                        tokio::time::sleep(CONNECT_FAIL_HOLD_DELAY).await;
                        return;
                    }

                    tokio::time::sleep(retry_wait).await;
                    retry_wait += CONNECT_RETRY_DELAY;
                }
            }
        };

        let (_remote, write, mut read) = connection.client_channels::<D>(settings.clone());

        if let Err(error) = self.handshake(&write, &mut read, settings.message_timeout).await {
            self.state.mark_peer_not_connected(self.remote_addr).await;
            self.drain_with_error(error).await;
            return;
        }

        self.state.mark_peer_connected(self.remote_addr).await;

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

        self.state.mark_peer_not_connected(self.remote_addr).await;
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
