use std::marker::PhantomData;
use std::time::Duration;

use message_encoding::MessageEncoding;
use sequenced_broadcast::{SequencedReceiver, SequencedSender};
use tokio::sync::mpsc::{channel, Sender};
use tracing::Instrument;

use crate::io::{SyncConnection, SyncIO};
use crate::message_io::{read_message_opt, send_message, send_zero_message};
use crate::state::DeterministicState;

use crate::recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails};
use crate::utils::{LogHelper, PanicHelper};
use super::{HandshakeError, MessageHelper};
use super::messages::*;

pub struct HandshakeServer<I: SyncIO> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
}

pub struct WantsState<I: SyncIO> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
}

pub struct WantsRecovery<I: SyncIO> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
    details: RecoverableStateDetails,
}

pub enum NewClient<I: SyncIO> {
    Fresh(WantsState<I>),
    WithData(WantsRecovery<I>),
}

impl<I: SyncIO> NewClient<I> {
    pub async fn send_state<D: DeterministicState + MessageEncoding>(self, state: &RecoverableState<D>) -> Result<ClientReady<I, D>, HandshakeError> {
        match self {
            Self::Fresh(s) => s.send_state(state).await,
            Self::WithData(s) => s.accept_client_with_state(state).await,
        }
    }
}

impl<I: SyncIO> HandshakeServer<I> {
    pub fn new(conn: SyncConnection<I>) -> Self {
        HandshakeServer {
            conn,
            buffer: Vec::with_capacity(1024),
        }
    }

    pub async fn accept(mut self) -> Result<NewClient<I>, HandshakeError> {
        let span = tracing::info_span!("HandshakeServer::accept");

        async {
            let Some(init) = I::read_msg(
                &mut self.buffer,
                &mut self.conn.read,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::ReadError).log()? else {
                return Err(HandshakeError::UnexpectedEmptyMessage).log();
            };

            let req = match init {
                HandshakeMessage::ConnectRequest(req) => req,
                other => return Err(HandshakeError::UnexpectedResponse(other)).log(),
            };

            match req {
                ConnectRequest::Connect => Ok(NewClient::Fresh(WantsState {
                    buffer: self.buffer,
                    conn: self.conn,
                })),
                ConnectRequest::Reconnect(details) => Ok(NewClient::WithData(WantsRecovery {
                    buffer: self.buffer,
                    conn: self.conn,
                    details
                })),
            }
        }.instrument(span).await
    }
}

impl<I: SyncIO> WantsState<I> {
    pub fn remote(&self) -> &I::Address {
        &self.conn.remote
    }

    pub async fn send_state<D: DeterministicState + MessageEncoding>(mut self, state: &RecoverableState<D>) -> Result<ClientReady<I, D>, HandshakeError> {
        let span = tracing::info_span!("WantsState::send_state", details = ?state.details());

        async {
            I::send(
                &mut self.buffer,
                &HandshakeMessage::ConnectResponse(ConnectResponse::AcceptConnection),
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError).log()?;

            send_message(
                &mut self.buffer,
                state,
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError).log()?;

            Ok(ClientReady {
                buffer: self.buffer,
                conn: self.conn,
                sequence: state.sequence(),
                _phantom: PhantomData,
            })
        }.instrument(span).await
    }
}

impl<I: SyncIO> WantsRecovery<I> {
    pub fn remote(&self) -> &I::Address {
        &self.conn.remote
    }

    pub fn details(&self) -> &RecoverableStateDetails {
        &self.details
    }

    pub async fn accept_recover<D: DeterministicState>(mut self, details: RecoverableStateDetails) -> Result<ClientReady<I, D>, HandshakeError> {
        let span = tracing::info_span!("WantsRecovery::accept_client");

        async {
            if !details.can_recover_follower(&self.details) {
                return Err(HandshakeError::RecoverError("leader cannot recover client")).log();
            }

            let sequence = self.details.sequence;

            I::send(
                &mut self.buffer,
                &HandshakeMessage::ConnectResponse(ConnectResponse::AcceptRecovery(details)),
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError).log()?;

            Ok(ClientReady {
                buffer: self.buffer,
                conn: self.conn,
                sequence,
                _phantom: PhantomData,
            })
        }.instrument(span).await
    }

    pub async fn accept_client_with_state<D: DeterministicState + MessageEncoding>(self, state: &RecoverableState<D>) -> Result<ClientReady<I, D>, HandshakeError> {
        let span = tracing::info_span!("WithData::accept_client_with_state", details = ?state.details());

        WantsState {
            buffer: self.buffer,
            conn: self.conn
        }
            .send_state(state)
            .instrument(span)
            .await
    }
}

pub struct ClientReady<I: SyncIO, D: DeterministicState> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
    sequence: u64,
    _phantom: PhantomData<D>,
}

impl<I: SyncIO, D: DeterministicState> ClientReady<I, D>
where D::Action: MessageEncoding, D::AuthorityAction: MessageEncoding
{
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn start_io_tasks(self, actions_tx: Sender<D::Action>) -> (I::Address, SequencedSender<RecoverableStateAction<D::AuthorityAction>>) {
        let (tx, rx) = channel(1024);

        let sequence = self.sequence;
        let remote = self.start_io_tasks2(actions_tx, SequencedReceiver::new(sequence, rx)).panic("invalid sequence");

        (
            remote,
            SequencedSender::new(sequence, tx)
        )
    }

    pub fn start_io_tasks2(self, actions_tx: Sender<D::Action>, mut authority_rx: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>) -> Result<I::Address, u64> {
        if authority_rx.next_seq() != self.sequence {
            tracing::error!("authority sequence: {} does not match recovery sequence: {}", authority_rx.next_seq(), self.sequence);
            return Err(self.sequence);
        }

        let Self {
            mut buffer,
            conn: SyncConnection { remote, mut read, mut write },
            ..
        } = self;

        /* read messages into action queue */
        tokio::spawn(async move {
            loop {
                match read_message_opt::<D::Action, _>(
                    &mut buffer,
                    &mut read,
                    Duration::from_secs(2),
                    Some(Duration::from_secs(8))
                ).await.err_log("got error reading next message from follower") {
                    Ok(Some(action)) => {
                        if actions_tx.send(action).await.is_err() {
                            tracing::info!("action queue closed for follower");
                            return;
                        }
                    }
                    Ok(None) => {}
                    Err(_) => return,
                }
            }
        }.instrument(tracing::info_span!("read from client", ?remote)));

        tokio::spawn(async move {
            let mut buffer = Vec::<u8>::with_capacity(1024);

            loop {
                match tokio::time::timeout(Duration::from_secs(2), authority_rx.recv()).await {
                    Err(_) => {
                        match tokio::time::timeout(Duration::from_secs(2), send_zero_message(&mut write)).await {
                            Ok(Ok(())) => continue,
                            Err(_) => {
                                tracing::error!("timeout sending zero message to follower");
                                return;
                            }
                            Ok(Err(error)) => {
                                tracing::error!(?error, "failed to send zero message to follower");
                                return;
                            }
                        }
                    }
                    Ok(None) => {
                        tracing::info!("sequenced queue closed for follower");
                        return;
                    }
                    Ok(Some(msg)) => {
                        match send_message(&mut buffer, &msg, &mut write, Duration::from_secs(2)).await {
                            Ok(()) => {}
                            Err(error) => {
                                tracing::error!(?error, "failed to send action to leader");
                                return;
                            }
                        }
                    }
                }
            }
        }.instrument(tracing::info_span!("write to client", ?remote)));

        Ok(remote)
    }
}
