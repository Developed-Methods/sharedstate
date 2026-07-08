//! Drives the local deterministic state from a fixed leader address.

use std::{
    io::{Error, ErrorKind},
    iter,
    sync::Arc,
    time::Duration,
};

use message_encoding::MessageEncoding;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;

use crate::{
    cluster::node_state::NodeState,
    protocol::messages::{SyncRequest, SyncResponse, PROTOCOL_VERSION},
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::{RecoverableStateAction, RecoverableStateDetails},
        subscribable_state::StateHandle,
    },
    transport::{channels::NetIoSettings, traits::SyncIO},
    utils::unique_state_id,
};

#[derive(Clone, Debug)]
pub struct StateSyncTiming {
    pub retry_delay: Duration,
}

impl Default for StateSyncTiming {
    fn default() -> Self {
        Self {
            retry_delay: Duration::from_millis(500),
        }
    }
}

pub struct StateSyncTask<I: SyncIO, D: DeterministicState> {
    state: Arc<NodeState<I::Address, D>>,
    io: Arc<I>,
    settings: NetIoSettings,
    actions_rx: Receiver<D::Action>,
    handle: StateHandle<D>,
    timing: StateSyncTiming,
}

enum SyncFlow {
    Retry,
    Shutdown,
}

impl<I, D> StateSyncTask<I, D>
where
    I: SyncIO,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    pub fn new(
        state: Arc<NodeState<I::Address, D>>,
        io: Arc<I>,
        settings: NetIoSettings,
        actions_rx: Receiver<D::Action>,
        timing: StateSyncTiming,
    ) -> Self {
        let handle = state.state.create_handle();
        Self {
            state,
            io,
            settings,
            actions_rx,
            handle,
            timing,
        }
    }

    pub async fn run(mut self) {
        if self.state.is_leader() {
            self.lead().await;
            return;
        }

        loop {
            self.state.set_connected_to_leader(false);
            match self.follow().await {
                SyncFlow::Retry => tokio::time::sleep(self.timing.retry_delay).await,
                SyncFlow::Shutdown => return,
            }
        }
    }

    async fn lead(&mut self) {
        let new_id = unique_state_id(&self.state.my_address);
        self.state
            .state
            .update(iter::once(RecoverableStateAction::BumpGeneration { new_id }))
            .await;
        self.state.set_connected_to_leader(true);
        tracing::info!("running as shared-state leader");

        while let Some(action) = self.actions_rx.recv().await {
            let authority = self
                .handle
                .read_with(move |state| state.authority(RecoverableStateAction::StateAction { action }));
            self.state.state.update(iter::once(authority)).await;
        }
    }

    async fn follow(&mut self) -> SyncFlow {
        let leader = self.state.leader_address;
        let connection = match tokio::time::timeout(self.settings.message_timeout, self.io.connect(&leader)).await {
            Ok(Ok(connection)) => connection,
            Ok(Err(error)) => {
                tracing::debug!(?leader, ?error, "failed to connect to leader");
                return SyncFlow::Retry;
            }
            Err(_) => {
                tracing::debug!(?leader, "timed out connecting to leader");
                return SyncFlow::Retry;
            }
        };

        let (_remote, write, mut read) = connection.client_channels::<D>(self.settings.clone());
        let details = self.state.state.settled_recovery_details().await;
        let mut next_seq = match self.subscribe(&write, &mut read, details).await {
            Ok(next_seq) => next_seq,
            Err(error) => {
                tracing::warn!(?leader, ?error, "leader subscription failed");
                return SyncFlow::Retry;
            }
        };

        self.state.set_connected_to_leader(true);
        tracing::info!(?leader, next_seq, "subscribed to shared-state leader");

        loop {
            tokio::select! {
                response = read.recv() => match response {
                    Some(SyncResponse::Action { seq, action }) => {
                        if seq != next_seq {
                            tracing::warn!(?leader, seq, next_seq, "leader action stream out of sequence");
                            return SyncFlow::Retry;
                        }
                        next_seq += 1;
                        self.state.state.update(iter::once(action)).await;
                    }
                    Some(response) => {
                        tracing::warn!(?leader, response = response.name(), "unexpected response from leader");
                        return SyncFlow::Retry;
                    }
                    None => {
                        tracing::info!(?leader, "leader subscription closed");
                        return SyncFlow::Retry;
                    }
                },
                action = self.actions_rx.recv() => {
                    let Some(action) = action else {
                        return SyncFlow::Shutdown;
                    };
                    if send(&write, SyncRequest::Action(action)).await.is_err() {
                        tracing::warn!(?leader, "failed to forward action to leader");
                        return SyncFlow::Retry;
                    }
                }
            }
        }
    }

    async fn subscribe(
        &mut self,
        write: &Sender<SyncRequest<D>>,
        read: &mut Receiver<SyncResponse<D>>,
        details: RecoverableStateDetails,
    ) -> std::io::Result<u64> {
        send(write, SyncRequest::ProtocolVersion(PROTOCOL_VERSION)).await?;
        expect_ok(recv(read, self.settings.message_timeout).await?, "protocol version")?;

        let local_next_seq = details.next_seq();
        send(write, SyncRequest::Subscribe(details)).await?;

        match recv(read, self.settings.message_timeout).await? {
            SyncResponse::Ok => Ok(local_next_seq),
            SyncResponse::FreshState(fresh) => {
                let next_seq = fresh.details().next_seq();
                self.state.state.reset(fresh).await;
                Ok(next_seq)
            }
            SyncResponse::NotConnected => Err(Error::new(ErrorKind::NotConnected, "sync source is not connected")),
            response => Err(unexpected("Ok or FreshState", &response)),
        }
    }
}

async fn send<D: DeterministicState>(write: &Sender<SyncRequest<D>>, request: SyncRequest<D>) -> std::io::Result<()> {
    write
        .send(request)
        .await
        .map_err(|error| Error::new(ErrorKind::BrokenPipe, format!("failed to send {:?}", error.0)))
}

async fn recv<D: DeterministicState>(
    read: &mut Receiver<SyncResponse<D>>,
    timeout: Duration,
) -> std::io::Result<SyncResponse<D>> {
    match tokio::time::timeout(timeout, read.recv()).await {
        Ok(Some(response)) => Ok(response),
        Ok(None) => Err(Error::new(ErrorKind::UnexpectedEof, "connection closed")),
        Err(_) => Err(Error::new(ErrorKind::TimedOut, "timed out waiting for response")),
    }
}

fn expect_ok<D: DeterministicState>(response: SyncResponse<D>, step: &'static str) -> std::io::Result<()> {
    match response {
        SyncResponse::Ok => Ok(()),
        response => {
            Err(Error::new(ErrorKind::InvalidData, format!("expected Ok during {step}, got {}", response.name())))
        }
    }
}

fn unexpected<D: DeterministicState>(expected: &str, response: &SyncResponse<D>) -> Error {
    Error::new(ErrorKind::InvalidData, format!("expected {expected}, got {}", response.name()))
}
