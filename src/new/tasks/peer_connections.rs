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
    protocol::messages::{SyncRequest, SyncResponse},
    state::determinstic_state::DeterministicState,
    transport::{
        channels::NetIoSettings,
        traits::{ClientChannels, SyncIO, SyncIOAddress},
    },
};

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

impl<I: SyncIO, D: DeterministicState> PeerConnections<I, D> {
    pub async fn send_rpc(
        &self,
        peer: I::Address,
        request: SyncRequest<I::Address, D>,
    ) -> Result<SyncResponse<I::Address, D>, PeerRpcError> {
        let (tx, rx) = oneshot::channel();
        let msg = RpcMessage { request, response: tx };

        let mut connections = self.connections.lock().await;
        let (msg, entry) = match connections.entry(peer) {
            hash_map::Entry::Occupied(mut o) => {
                let conn = o.get_mut();

                match conn.tx.try_send(msg) {
                    Ok(_) => {
                        drop(connections);

                        return match rx.await {
                            Ok(res) => res,
                            Err(_) => Err(PeerRpcError::ResponseDropped),
                        };
                    }
                    Err(TrySendError::Closed(og_msg)) => (og_msg, hash_map::Entry::Occupied(o)),
                    Err(TrySendError::Full(og_msg)) => {
                        let sender = conn.tx.clone();
                        drop(connections);
                        sender.send(og_msg).await;

                        return match rx.await {
                            Ok(res) => res,
                            Err(_) => Err(PeerRpcError::ResponseDropped),
                        };
                    }
                }
            }
            entry => (msg, entry),
        };

        let conn = Connection::create(entry.key().clone(), self.io.clone(), self.conn_settings.clone());
        entry
            .insert_entry(conn)
            .get()
            .tx
            .try_send(msg)
            .expect("failed to send packet to new connection");

        match rx.await {
            Ok(res) => res,
            Err(_) => Err(PeerRpcError::ResponseDropped),
        }
    }
}

impl<A: SyncIOAddress, D: DeterministicState> Connection<A, D> {
    pub fn create<I: SyncIO<Address = A>>(addr: A, io: Arc<I>, settings: NetIoSettings) -> Self {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub enum PeerRpcError {
    RequestNotAllowedOverRpc,
    FailedToConnectToPeer,
    ResponseDropped,
}

struct ConnectionWorker<A: SyncIOAddress, D: DeterministicState> {
    rx: Receiver<RpcMessage<A, D>>,
    remote_addr: A,
}

impl<A: SyncIOAddress, D: DeterministicState> ConnectionWorker<A, D>
where
    D: MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    async fn run<I: SyncIO<Address = A>>(mut self, io: I, settings: NetIoSettings) {
        let mut repeat_failures = 0;

        let connect_res: Result<ClientChannels<I, D>, PeerRpcError> = loop {
            let connection = match io.connect(&self.remote_addr).await {
                Ok(v) => v,
                Err(error) => {
                    tracing::error!(?error, repeat_failures, "failed to connect to peer {:?}", self.remote_addr);
                    repeat_failures += 1;

                    if repeat_failures > 3 {
                        break Err(PeerRpcError::FailedToConnectToPeer);
                    }

                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };

            break Ok(connection.client_channels(settings));
        };

        let client = match connect_res {
            Ok(v) => v,
            Err(error) => {
                self.rx.close();

                while let Some(msg) = self.rx.recv().await {
                    msg.response.send(Err(error.clone()));
                }

                return;
            }
        };

        while let Some(msg) = self.rx.recv().await {}
    }
}
