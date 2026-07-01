use std::{collections::hash_map, num::NonZeroU64, sync::Arc};

use message_encoding::MessageEncoding;
use sequenced_broadcast::SequencedReceiver;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};

use crate::{
    new::{
        node_state::{NodeState, PeerState},
        subscribable_state::StateHandle,
    },
    protocol::messages::{SharePeerDetails, SyncRequest, SyncResponse, PROTOCOL_VERSION},
    state::{
        determinstic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction},
    },
    transport::{
        channels::NetIoSettings,
        traits::{SyncConnection, SyncIO, SyncIOAddress, SyncIOListener},
    },
    utils::now_ms,
};

pub struct RpcServer<A: SyncIOAddress, D: DeterministicState> {
    state: Arc<NodeState<A, D>>,
    state_handle: Mutex<StateHandle<D>>,
    actions_tx: Sender<(A, D::Action)>,
}

impl<A: SyncIOAddress, D: DeterministicState> RpcServer<A, D> {
    pub fn new(state: Arc<NodeState<A, D>>, actions_tx: Sender<(A, D::Action)>) -> Self {
        let state_handle = Mutex::new(state.state.create_handle());

        RpcServer {
            state,
            state_handle,
            actions_tx,
        }
    }

    pub async fn handle(&self, peer_addr: A, request: SyncRequest<A, D>) -> ResponseOrFeed<A, D> {
        {
            let mut peers = self.state.peers.lock().await;

            if let Some(state) = peers.get_mut(&peer_addr) {
                state.last_global_connectivity = NonZeroU64::new(now_ms());
            } else {
                tracing::error!("got message from peer but they are not in state");
            }
        }

        let resp = match request {
            SyncRequest::ProtocolVersion(_) => SyncResponse::UnexpectedRequest,
            SyncRequest::MyAddress(_) => SyncResponse::UnexpectedRequest,

            SyncRequest::ShareLeaderPath => match self.state.leader_status.path_to_leader().await {
                Some(v) => SyncResponse::LeaderPath(v),
                None => SyncResponse::NoPathToLeader,
            },

            SyncRequest::ShareLeaderInfo => {
                let peers = self.state.peers.lock().await;
                let recover_details = self.state_handle.lock().await.recover_details();
                let mut reachable_can_lead = peers
                    .iter()
                    .filter_map(|(_, peer)| (peer.is_connected && peer.can_lead == Some(true)).then_some(peer.addr))
                    .collect::<Vec<_>>();
                if self.state.can_lead {
                    reachable_can_lead.push(self.state.my_address);
                }
                drop(peers);

                SyncResponse::LeaderInfo(
                    self.state
                        .leader_status
                        .local_observation(self.state.can_lead, reachable_can_lead, recover_details)
                        .await,
                )
            }
            SyncRequest::Action { source, action } => {
                if self.actions_tx.try_send((source, action)).is_ok() {
                    SyncResponse::Ok
                } else {
                    SyncResponse::FailedToQueueAction { source }
                }
            }
            SyncRequest::LeaderInformation { source, info } => {
                let mut peers = self.state.peers.lock().await;
                match peers.entry(source) {
                    hash_map::Entry::Occupied(o) => {
                        o.into_mut().leader_observation = Some(info);
                    }
                    hash_map::Entry::Vacant(entry) => {
                        entry.insert(PeerState {
                            addr: info.observer,
                            latency: None,
                            can_lead: Some(info.can_lead),
                            is_connected: false,
                            last_global_connectivity: NonZeroU64::new(now_ms()),
                            leader_observation: Some(info),
                        });
                    }
                }

                SyncResponse::Ok
            }
            SyncRequest::SubscribeRecovery(details) => match self.state.state.subscribe(details).await {
                Ok(feed) => return ResponseOrFeed::Subscription { feed },
                Err(error) => {
                    tracing::warn!(?error, "client recovery failed");
                    SyncResponse::RecoveryFailed
                }
            },
            SyncRequest::SubscribeFresh => {
                let (state, feed) = self.state.state.subscribe_fresh().await;
                return ResponseOrFeed::FreshState { state, feed };
            }
            SyncRequest::Ping(id) => SyncResponse::Pong(id),
            SyncRequest::SharePeers(shared_peers) => {
                let mut peers = self.state.peers.lock().await;

                for peer in shared_peers {
                    match peers.entry(peer.address) {
                        hash_map::Entry::Occupied(o) => {
                            let state = o.into_mut();

                            if let Some(can_lead) = peer.can_be_leader {
                                state.can_lead = Some(can_lead);
                            }

                            state.last_global_connectivity =
                                match (state.last_global_connectivity, peer.last_global_activity) {
                                    (None, Some(a)) | (Some(a), None) => Some(a),
                                    (Some(a), Some(b)) => Some(a.max(b)),
                                    _ => None,
                                };
                        }
                        hash_map::Entry::Vacant(v) => {
                            v.insert(PeerState {
                                addr: peer.address,
                                latency: None,
                                can_lead: peer.can_be_leader,
                                is_connected: false,
                                last_global_connectivity: peer.last_global_activity,
                                leader_observation: None,
                            });
                        }
                    }
                }

                let share_peer_details = peers
                    .iter()
                    .map(|(_, peer)| SharePeerDetails {
                        address: peer.addr,
                        can_be_leader: peer.can_lead,
                        last_global_activity: peer.last_global_connectivity,
                    })
                    .collect::<Vec<_>>();

                SyncResponse::Peers(share_peer_details)
            }
        };

        ResponseOrFeed::Response(resp)
    }
}

impl<A, D> RpcServer<A, D>
where
    A: SyncIOAddress,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    pub fn start_listener<I>(self: Arc<Self>, io: Arc<I>, settings: NetIoSettings) -> JoinHandle<()>
    where
        I: SyncIOListener<Address = A>,
    {
        tokio::spawn(async move {
            loop {
                match io.next_client().await {
                    Ok(conn) => {
                        let server = self.clone();
                        let settings = settings.clone();
                        tokio::spawn(async move {
                            server.handle_client(conn, settings).await;
                        });
                    }
                    Err(error) => {
                        tracing::warn!(?error, "rpc listener stopped accepting clients");
                        break;
                    }
                }
            }
        })
    }

    pub async fn handle_client<I>(self: Arc<Self>, conn: SyncConnection<I>, settings: NetIoSettings)
    where
        I: SyncIO<Address = A>,
    {
        let (transport_addr, write, mut read) = conn.server_channels::<D>(settings.clone());
        let Some(peer_addr) = handshake_client(&write, &mut read, settings.message_timeout).await else {
            tracing::debug!(?transport_addr, "rpc client handshake failed");
            return;
        };

        while let Some(request) = read.recv().await {
            match self.handle(peer_addr, request).await {
                ResponseOrFeed::Response(response) => {
                    if write.send(response).await.is_err() {
                        break;
                    }
                }
                ResponseOrFeed::FreshState { state, feed } => {
                    if write.send(SyncResponse::FreshState(state)).await.is_err() {
                        break;
                    }
                    stream_feed(write, feed).await;
                    break;
                }
                ResponseOrFeed::Subscription { feed } => {
                    if write.send(SyncResponse::Accepted(feed.next_seq())).await.is_err() {
                        break;
                    }
                    stream_feed(write, feed).await;
                    break;
                }
            }
        }
    }
}

async fn handshake_client<A, D>(
    write: &Sender<SyncResponse<A, D>>,
    read: &mut Receiver<SyncRequest<A, D>>,
    timeout: std::time::Duration,
) -> Option<A>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    let version = tokio::time::timeout(timeout, read.recv()).await.ok().flatten()?;
    match version {
        SyncRequest::ProtocolVersion(PROTOCOL_VERSION) => {
            write.send(SyncResponse::Ok).await.ok()?;
        }
        _ => {
            let _ = write.send(SyncResponse::UnexpectedRequest).await;
            return None;
        }
    }

    let address = tokio::time::timeout(timeout, read.recv()).await.ok().flatten()?;
    match address {
        SyncRequest::MyAddress(address) => {
            write.send(SyncResponse::Ok).await.ok()?;
            Some(address)
        }
        _ => {
            let _ = write.send(SyncResponse::UnexpectedRequest).await;
            None
        }
    }
}

async fn stream_feed<A, D>(
    write: Sender<SyncResponse<A, D>>,
    mut feed: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
) where
    A: SyncIOAddress,
    D: DeterministicState,
{
    loop {
        match feed.recv().await {
            Ok((seq, action)) => {
                if write.send(SyncResponse::AuthorityAction(seq, action)).await.is_err() {
                    break;
                }
            }
            Err(error) => {
                tracing::debug!(?error, "rpc subscription feed closed");
                let _ = write.send(SyncResponse::ActionStreamClosed).await;
                break;
            }
        }
    }
}

pub enum ResponseOrFeed<A: SyncIOAddress, D: DeterministicState> {
    Response(SyncResponse<A, D>),
    FreshState {
        state: RecoverableState<D>,
        feed: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
    },
    Subscription {
        feed: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
    },
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Result, sync::Arc, time::Duration};

    use message_encoding::MessageEncoding;
    use sequenced_broadcast::SequencedBroadcastSettings;
    use tokio::{
        io::{duplex, split, DuplexStream, ReadHalf, WriteHalf},
        sync::{mpsc, Mutex},
    };

    use super::*;
    use crate::{
        new::{subscribable_state::SubscribableState, tasks::current_leader::CurrentLeaderStatus},
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

    struct TestIo;

    impl SyncIO for TestIo {
        type Address = u64;
        type Read = ReadHalf<DuplexStream>;
        type Write = WriteHalf<DuplexStream>;

        async fn connect(&self, _remote: &Self::Address) -> Result<SyncConnection<Self>> {
            unreachable!("rpc server tests use direct duplex connections")
        }
    }

    fn settings() -> NetIoSettings {
        NetIoSettings {
            process_timeout: Duration::from_millis(100),
            message_timeout: Duration::from_millis(500),
        }
    }

    fn node_state(address: u64) -> Arc<NodeState<u64, TestState>> {
        let mut peers = HashMap::new();
        peers.insert(
            2,
            PeerState {
                addr: 2,
                latency: None,
                can_lead: Some(true),
                is_connected: true,
                last_global_connectivity: None,
                leader_observation: None,
            },
        );

        Arc::new(NodeState {
            my_address: address,
            can_lead: true,
            peers: Mutex::new(peers),
            state: SubscribableState::new(
                RecoverableState::new(address, TestState(1)),
                SequencedBroadcastSettings::default(),
            )
            .unwrap(),
            leader_status: Arc::new(CurrentLeaderStatus::new(address)),
        })
    }

    fn duplex_connections() -> (SyncConnection<TestIo>, SyncConnection<TestIo>) {
        let (client, server) = duplex(4096);
        let (client_read, client_write) = split(client);
        let (server_read, server_write) = split(server);

        (
            SyncConnection {
                remote: 1,
                read: client_read,
                write: client_write,
            },
            SyncConnection {
                remote: 2,
                read: server_read,
                write: server_write,
            },
        )
    }

    async fn recv_response(read: &mut Receiver<SyncResponse<u64, TestState>>) -> SyncResponse<u64, TestState> {
        tokio::time::timeout(Duration::from_secs(1), read.recv())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn handle_client_handshakes_then_dispatches_requests() {
        let (actions_tx, _actions_rx) = mpsc::channel(16);
        let server = Arc::new(RpcServer::new(node_state(1), actions_tx));
        let (client_conn, server_conn) = duplex_connections();
        let server_task = tokio::spawn(server.handle_client(server_conn, settings()));

        let (_remote, write, mut read) = client_conn.client_channels::<TestState>(settings());

        write
            .send(SyncRequest::ProtocolVersion(PROTOCOL_VERSION))
            .await
            .unwrap();
        assert!(matches!(recv_response(&mut read).await, SyncResponse::Ok));

        write.send(SyncRequest::MyAddress(2)).await.unwrap();
        assert!(matches!(recv_response(&mut read).await, SyncResponse::Ok));

        write.send(SyncRequest::Ping(42)).await.unwrap();
        assert!(matches!(recv_response(&mut read).await, SyncResponse::Pong(42)));

        drop(write);
        server_task.await.unwrap();
    }
}
