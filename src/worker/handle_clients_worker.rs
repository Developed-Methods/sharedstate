use sequenced_broadcast::SequencedBroadcast;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

use crate::{io::{SyncConnection, SyncIO}, state::{DeterministicState, SharedState}};

pub struct HandleClientsWorker<I: SyncIO, D: DeterministicState> {
    state: SharedState<D>,
    listener: Receiver<SyncConnection<I>>,
}

pub struct HandleClientsWorkerRun<I: SyncIO, D: DeterministicState> {
    state: SharedState<D>,
    listener: Receiver<SyncConnection<I>>,
}

impl<I: SyncIO, D: DeterministicState> HandleClientsWorker<I, D> where D::AuthorityAction: Clone {
    pub async fn accept_clients(&mut self, broadcast: SequencedBroadcast<D::AuthorityAction>) {
        self.listener.recv()
    }
}
