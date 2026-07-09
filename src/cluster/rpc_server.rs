use std::sync::Arc;

use arc_metrics::helpers::ActiveGauge;
use message_encoding::MessageEncoding;
use sequenced_broadcast::SequencedReceiver;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    cluster::node_state::NodeState,
    metrics::SharedStateMetrics,
    protocol::messages::{SyncRequest, SyncResponse, PROTOCOL_VERSION},
    state::{deterministic_state::DeterministicState, recoverable_state::RecoverableStateAction},
    transport::{
        channels::NetIoSettings,
        traits::{SyncConnection, SyncIO, SyncIOAddress, SyncIOListener},
    },
};

pub struct RpcServer<A: SyncIOAddress, D: DeterministicState> {
    state: Arc<NodeState<A, D>>,
    actions_tx: Sender<D::Action>,
    metrics: Arc<SharedStateMetrics>,
}

impl<A: SyncIOAddress, D: DeterministicState> RpcServer<A, D> {
    pub fn new(state: Arc<NodeState<A, D>>, actions_tx: Sender<D::Action>, metrics: Arc<SharedStateMetrics>) -> Self {
        Self {
            state,
            actions_tx,
            metrics,
        }
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

        if !handshake_client(&write, &mut read, settings.message_timeout).await {
            tracing::debug!(?transport_addr, "rpc client handshake failed");
            return;
        }

        let request = loop {
            let Some(request) = read.recv().await else {
                return;
            };
            match request {
                SyncRequest::Ping(id) => {
                    if write.send(SyncResponse::Pong(id)).await.is_err() {
                        return;
                    }
                }
                request => break request,
            }
        };

        let (feed, fresh_state) = match request {
            SyncRequest::Subscribe(details) => {
                if !self.state.is_leader() && !self.state.is_connected_to_leader() {
                    let _ = write.send(SyncResponse::NotConnected).await;
                    return;
                }

                /* note keep lock while dealing with subscribe */
                match self.state.state.subscribe(details).await {
                    Ok(feed) => (feed, None),
                    Err(error) => {
                        tracing::debug!(?error, "client recovery failed, sending fresh state");
                        let (state, feed) = self.state.state.subscribe_fresh().await;
                        (feed, Some(state))
                    }
                }
            }
            SyncRequest::Action(action) => {
                self.handle_action(action).await;
                let _ = write.send(SyncResponse::Ok).await;
                return;
            }
            SyncRequest::ProtocolVersion(_) => {
                let _ = write.send(SyncResponse::Ok).await;
                return;
            }
            SyncRequest::Ping(_) => unreachable!("ping requests are handled before subscription dispatch"),
        };

        if let Some(state) = fresh_state {
            if write.send(SyncResponse::FreshState(state)).await.is_err() {
                return;
            }
        } else if write.send(SyncResponse::Ok).await.is_err() {
            return;
        }

        self.serve_subscription(write, read, feed).await;
    }

    async fn serve_subscription(
        &self,
        write: Sender<SyncResponse<D>>,
        mut read: Receiver<SyncRequest<D>>,
        mut feed: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
    ) {
        let _active = ActiveGauge::new(&self.metrics, |metrics| &metrics.active_subscription_count);

        loop {
            tokio::select! {
                action = feed.recv() => match action {
                    Ok((seq, action)) => {
                        if write.send(SyncResponse::Action { seq, action }).await.is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        tracing::debug!(?error, "rpc subscription feed closed");
                        break;
                    }
                },
                request = read.recv() => match request {
                    Some(SyncRequest::Action(action)) => self.handle_action(action).await,
                    Some(SyncRequest::Ping(id)) => {
                        if write.send(SyncResponse::Pong(id)).await.is_err() {
                            break;
                        }
                    }
                    Some(request) => tracing::debug!(?request, "ignoring non-action request after subscription"),
                    None => break,
                },
            }
        }
    }

    async fn handle_action(&self, action: D::Action) {
        self.metrics.action_client_count.inc();
        if self.actions_tx.send(action).await.is_err() {
            tracing::warn!("failed to queue client action");
        }
    }
}

async fn handshake_client<D>(
    write: &Sender<SyncResponse<D>>,
    read: &mut Receiver<SyncRequest<D>>,
    timeout: std::time::Duration,
) -> bool
where
    D: DeterministicState,
{
    let version = tokio::time::timeout(timeout, read.recv()).await.ok().flatten();
    match version {
        Some(SyncRequest::ProtocolVersion(PROTOCOL_VERSION)) => write.send(SyncResponse::Ok).await.is_ok(),
        _ => {
            let _ = write.send(SyncResponse::NotConnected).await;
            false
        }
    }
}
