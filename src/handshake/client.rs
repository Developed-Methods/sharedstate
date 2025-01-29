use std::{marker::PhantomData, time::Duration};

use message_encoding::MessageEncoding;
use sequenced_broadcast::{SequencedReceiver, SequencedSender, SequencedSenderError};
use tokio::{sync::mpsc::{channel, Sender}, task::JoinHandle};
use tracing::Instrument;

use crate::{io::{SyncConnection, SyncIO}, message_io::{read_message, read_message_opt, send_message, send_zero_message}, recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails}, state::DeterministicState, utils::PanicHelper};
use super::{HandshakeError, MessageHelper};
use crate::utils::LogHelper;
use super::messages::*;


pub struct HandshakeClient<I: SyncIO> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
}

pub enum ConnectionEstablished<I: SyncIO> {
    FreshConnection(FreshConnection<I>),
    RecoveringConnection(RecoveringConnection<I>),
}

pub struct FreshConnection<I: SyncIO> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
}

pub struct RecoveringConnection<I: SyncIO> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
    local_follower_details: RecoverableStateDetails,
    leader_details: RecoverableStateDetails,
}

impl<I: SyncIO> HandshakeClient<I> {
    pub fn new(conn: SyncConnection<I>) -> Self {
        HandshakeClient {
            buffer: Vec::with_capacity(1024),
            conn,
        }
    }

    pub async fn connect(self) -> Result<ConnectionEstablished<I>, HandshakeError> {
        self.reconnect_opt(None).await
    }

    pub async fn reconnect(self, recover: RecoverableStateDetails) -> Result<ConnectionEstablished<I>, HandshakeError> {
        self.reconnect_opt(Some(recover)).await
    }

    pub async fn reconnect_opt(mut self, reconnect: Option<RecoverableStateDetails>) -> Result<ConnectionEstablished<I>, HandshakeError> {
        let span = tracing::info_span!("HandshakeClient::connect", reconnect = reconnect.is_some());

        async {
            if let Some(recover) = reconnect {
                I::send(
                    &mut self.buffer,
                    &HandshakeMessage::ConnectRequest(ConnectRequest::Reconnect(recover.clone())),
                    &mut self.conn.write,
                    Duration::from_secs(2)
                ).await.map_err(HandshakeError::SendError).log()?;

                let recv = I::read_msg(
                    &mut self.buffer,
                    &mut self.conn.read,
                    Duration::from_secs(2)
                ).await.map_err(HandshakeError::ReadError).log()?;

                let response = match recv {
                    Some(HandshakeMessage::ConnectResponse(response)) => response,
                    Some(other) => return Err(HandshakeError::UnexpectedResponse(other)).log(),
                    None => return Err(HandshakeError::UnexpectedEmptyMessage).log(),
                };

                return match response {
                    ConnectResponse::AcceptRecovery(details) => {
                        if !details.can_recover_follower(&recover) {
                            return Err(HandshakeError::RecoverError("server accepted recovery but send recovery details that are not supported")).log();
                        }

                        Ok(ConnectionEstablished::RecoveringConnection(RecoveringConnection {
                            buffer: self.buffer,
                            conn: self.conn,
                            local_follower_details: recover,
                            leader_details: details,
                        }))
                    },
                    ConnectResponse::AcceptConnection => Ok(ConnectionEstablished::FreshConnection(FreshConnection {
                        buffer: self.buffer,
                        conn: self.conn,
                    })),
                    ConnectResponse::RejectConnection => Err(HandshakeError::ConnectionRejected).log(),
                };
            }

            I::send(
                &mut self.buffer,
                &HandshakeMessage::ConnectRequest(ConnectRequest::Connect),
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError).log()?;

            let recv = I::read_msg(
                &mut self.buffer,
                &mut self.conn.read,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::ReadError).log()?;

            let response = match recv {
                Some(HandshakeMessage::ConnectResponse(response)) => response,
                Some(other) => return Err(HandshakeError::UnexpectedResponse(other)).log(),
                None => return Err(HandshakeError::UnexpectedEmptyMessage).log(),
            };

            match response {
                ConnectResponse::AcceptConnection => Ok(ConnectionEstablished::FreshConnection(FreshConnection {
                    buffer: self.buffer,
                    conn: self.conn,
                })),
                ConnectResponse::RejectConnection => Err(HandshakeError::ConnectionRejected).log(),
                other => Err(HandshakeError::UnexpectedResponse(HandshakeMessage::ConnectResponse(other))).log(),
            }
        }.instrument(span).await
    }
}

pub struct ConnectedToLeader<I: SyncIO, D: DeterministicState> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
    sequence: u64,
    _phantom: PhantomData<D>,
}

impl<I: SyncIO> FreshConnection<I> {
    pub async fn load_state<D: DeterministicState + MessageEncoding>(mut self) -> Result<(ConnectedToLeader<I, D>, RecoverableState<D>), HandshakeError> {
        let span = tracing::info_span!("FreshConnection::load_state");

        async {
            let Some(state) = read_message::<RecoverableState<D>, _>(
                &mut self.buffer,
                &mut self.conn.read,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::ReadError).log()? else {
                return Err(HandshakeError::UnexpectedEmptyMessage).log();
            };

            Ok((
                ConnectedToLeader {
                    buffer: self.buffer,
                    conn: self.conn,
                    sequence: state.sequence(),
                    _phantom: PhantomData,
                },
                state
            ))
        }.instrument(span).await
    }
}

impl<I: SyncIO> RecoveringConnection<I> {
    pub fn sequence(&self) -> u64 {
        self.local_follower_details.sequence
    }

    pub fn recover<D: DeterministicState + Clone>(self, state: &mut RecoverableState<D>) -> Result<ConnectedToLeader<I, D>, HandshakeError> {
        if !self.local_follower_details.eq(state.details()) {
            return Err(HandshakeError::RecoverError("state changed before recovery")).log();
        }

        if !state.upgrade(&self.leader_details) {
            panic!("cannot update state based on leader details, should have been validated earlier in library");
        }

        Ok(ConnectedToLeader {
            buffer: self.buffer,
            conn: self.conn,
            sequence: state.sequence(),
            _phantom: PhantomData,
        })
    }
}

pub struct LeaderChannels<D: DeterministicState> {
    pub action_tx: Sender<D::Action>,
    pub authority_rx: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
    pub reader_task: JoinHandle<SequencedSender<RecoverableStateAction<D::AuthorityAction>>>,
}

impl<I: SyncIO, D: DeterministicState> ConnectedToLeader<I, D> 
    where D::Action: MessageEncoding, D::AuthorityAction: MessageEncoding
{
    pub fn start_io_workers2(self, mut authority_tx: SequencedSender<RecoverableStateAction<D::AuthorityAction>>) -> Result<(I::Address, Sender<D::Action>, JoinHandle<SequencedSender<RecoverableStateAction<D::AuthorityAction>>>), u64> {
        if authority_tx.seq() != self.sequence {
            return Err(self.sequence);
        }

        let Self {
            mut buffer,
            conn: SyncConnection { remote, mut read, mut write },
            ..
        } = self;

        /* read messages into authority queue */
        let read_task = tokio::spawn(async move {
            loop {
                match read_message_opt::<(u64, RecoverableStateAction<D::AuthorityAction>), _>(&mut buffer, &mut read, Duration::from_secs(2), Some(Duration::from_secs(8))).await {
                    Ok(Some((seq, action))) => {
                        match authority_tx.safe_send(seq, action).await {
                            Ok(_) => {}
                            Err(SequencedSenderError::ChannelClosed(_)) => {
                                tracing::info!("sequenced sender for leader connection closed");
                                break;
                            }
                            Err(SequencedSenderError::InvalidSequence(actual, _)) => {
                                tracing::error!("leader sent wrong sequence, expected: {} but got: {}", actual, seq);
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                    }
                    Err(error) => {
                        tracing::error!(?error, "got error reading next message from leader");
                        break;
                    }
                }
            }

            authority_tx
        }.instrument(tracing::info_span!("read from leader", ?remote)));

        let (action_tx, mut action_rx) = channel::<D::Action>(1024);

        /* send actions to leader */
        tokio::spawn(async move {
            let mut buffer = Vec::<u8>::with_capacity(1024);

            loop {
                match tokio::time::timeout(Duration::from_secs(2), action_rx.recv()).await {
                    Err(_) => {
                        match tokio::time::timeout(Duration::from_secs(2), send_zero_message(&mut write)).await {
                            Ok(Ok(())) => continue,
                            Err(_) => {
                                tracing::error!("timeout sending zero message to leader");
                                return;
                            }
                            Ok(Err(error)) => {
                                tracing::error!(?error, "failed to send zero message to leader");
                                return;
                            }
                        }
                    }
                    Ok(None) => {
                        tracing::info!("action_rx empty, closing sender");
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
        }.instrument(tracing::info_span!("write to leader", ?remote)));

        Ok((remote, action_tx, read_task))
    }

    pub fn start_io_workers(self) -> (I::Address, LeaderChannels<D>) {
        let (authority_tx, authority_rx) = channel(1024);
        let authority_tx = SequencedSender::new(self.sequence, authority_tx);
        let authority_rx = SequencedReceiver::new(self.sequence, authority_rx);

        let (
            addr,
            action_tx,
            reader_task
        ) = self.start_io_workers2(authority_tx).panic("invalid sequence");

        (
            addr,
            LeaderChannels {
                action_tx,
                authority_rx,
                reader_task
            },
        )
    }
}
