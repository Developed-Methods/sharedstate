use std::sync::Arc;

use message_encoding::MessageEncoding;
use sequenced_broadcast::SequencedReceiver;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    cluster::node_state::{NodeState, PeerState},
    protocol::messages::{LeadershipEpoch, PROTOCOL_VERSION, SyncRequest, SyncResponse},
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction},
    },
    transport::{
        channels::NetIoSettings,
        traits::{SyncConnection, SyncIO, SyncIOAddress, SyncIOListener},
    },
};

pub struct RpcServer<A: SyncIOAddress, D: DeterministicState> {
    state: Arc<NodeState<A, D>>,
    actions_tx: Sender<(A, D::Action)>,
}

impl<A: SyncIOAddress, D: DeterministicState> RpcServer<A, D> {
    pub fn new(state: Arc<NodeState<A, D>>, actions_tx: Sender<(A, D::Action)>) -> Self {
        RpcServer { state, actions_tx }
    }

    pub async fn handle(&self, peer_addr: A, request: SyncRequest<A, D>) -> ResponseOrFeed<A, D> {
        if !self.state.note_known_peer_activity(peer_addr).await {
            tracing::debug!(?peer_addr, "learned about new peer from inbound connection");
        }

        let resp = match request {
            SyncRequest::ProtocolVersion(_) => SyncResponse::UnexpectedRequest,
            SyncRequest::MyAddress(_) => SyncResponse::UnexpectedRequest,

            SyncRequest::Action { source, action } => {
                if self.actions_tx.send((source, action)).await.is_ok() {
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
            SyncRequest::RecoverySnapshot => {
                self.state.state.settled_recovery_details().await;
                let (snapshot, _) = self.state.state.subscribe_fresh().await;
                SyncResponse::RecoverySnapshot(snapshot)
            }
            SyncRequest::SubscribeRecovery(details) => {
                let Some(epoch) = self.state.replication_epoch() else {
                    return ResponseOrFeed::Response(SyncResponse::RecoveryFailed);
                };
                match self.state.state.subscribe(details).await {
                    Ok(feed) if self.state.replication_epoch() == Some(epoch) => {
                        return ResponseOrFeed::Subscription { epoch, feed };
                    }
                    _ => SyncResponse::RecoveryFailed,
                }
            }
            SyncRequest::SubscribeFresh => {
                let Some(epoch) = self.state.replication_epoch() else {
                    return ResponseOrFeed::Response(SyncResponse::RecoveryFailed);
                };
                let (state, feed) = self.state.state.subscribe_fresh().await;
                if self.state.replication_epoch() != Some(epoch) {
                    return ResponseOrFeed::Response(SyncResponse::RecoveryFailed);
                }
                return ResponseOrFeed::FreshState { epoch, state, feed };
            }
            SyncRequest::LeaderQuery => SyncResponse::LeaderState(self.state.current_leader()),
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
                        tracing::warn!(?error, "failed to accept client");
                        continue;
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
                ResponseOrFeed::FreshState { epoch, state, feed } => {
                    if send_validated(&self.state, epoch, &write, SyncResponse::FreshState(epoch, state))
                        .await
                        .is_err()
                    {
                        break;
                    }
                    stream_feed(self.state.clone(), epoch, write, feed).await;
                    break;
                }
                ResponseOrFeed::Subscription { epoch, feed } => {
                    if send_validated(&self.state, epoch, &write, SyncResponse::Accepted(epoch, feed.next_seq()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                    stream_feed(self.state.clone(), epoch, write, feed).await;
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
    state: Arc<NodeState<A, D>>,
    epoch: LeadershipEpoch,
    write: Sender<SyncResponse<A, D>>,
    mut feed: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
) where
    A: SyncIOAddress,
    D: DeterministicState,
{
    loop {
        if state.replication_epoch() != Some(epoch) {
            break;
        }
        let received = tokio::select! {
            received = feed.recv() => received,
            _ = state.leadership_changed.notified() => continue,
            _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => continue,
        };
        match received {
            Ok((seq, action)) => {
                if send_validated(&state, epoch, &write, SyncResponse::AuthorityAction(epoch, seq, action))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Err(error) => {
                tracing::debug!(?error, "rpc subscription feed closed");
                let _ = write.try_send(SyncResponse::ActionStreamClosed);
                break;
            }
        }
    }
}

async fn send_validated<A: SyncIOAddress, D: DeterministicState>(
    state: &NodeState<A, D>,
    epoch: LeadershipEpoch,
    write: &Sender<SyncResponse<A, D>>,
    response: SyncResponse<A, D>,
) -> Result<(), ()> {
    let reserve = write.reserve();
    tokio::pin!(reserve);
    loop {
        if state.replication_epoch() != Some(epoch) {
            return Err(());
        }
        tokio::select! {
            permit = &mut reserve => {
                let permit = permit.map_err(|_| ())?;
                if state.replication_epoch() != Some(epoch) {
                    return Err(());
                }
                permit.send(response);
                return Ok(());
            }
            _ = state.leadership_changed.notified() => {},
            _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {},
        }
    }
}

pub enum ResponseOrFeed<A: SyncIOAddress, D: DeterministicState> {
    Response(SyncResponse<A, D>),
    FreshState {
        epoch: LeadershipEpoch,
        state: RecoverableState<D>,
        feed: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
    },
    Subscription {
        epoch: LeadershipEpoch,
        feed: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
    },
}
