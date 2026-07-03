use std::sync::Arc;

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
    protocol::messages::{SyncRequest, SyncResponse, PROTOCOL_VERSION},
    state::{
        determinstic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction},
    },
    transport::{
        channels::NetIoSettings,
        traits::{SyncConnection, SyncIO, SyncIOAddress, SyncIOListener},
    },
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
        if !self.state.note_known_peer_activity(peer_addr).await {
            tracing::error!("got message from peer but they are not in state");
        }

        let resp = match request {
            SyncRequest::ProtocolVersion(_) => SyncResponse::UnexpectedRequest,
            SyncRequest::MyAddress(_) => SyncResponse::UnexpectedRequest,

            SyncRequest::Action { source, action } => {
                if self.actions_tx.try_send((source, action)).is_ok() {
                    SyncResponse::Ok
                } else {
                    SyncResponse::FailedToQueueAction { source }
                }
            }
            SyncRequest::LeaderInformation(info) => {
                let mut lock = self.state.peers.lock().await;
                let peer = lock.entry(peer_addr).or_insert_with(|| PeerState::empty(peer_addr));
                peer.can_lead = Some(info.can_lead);
                peer.leader_info = Some(info);

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
                self.state.merge_peer_details(shared_peers).await;
                let share_peer_details = self.state.known_peer_details().await;
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
