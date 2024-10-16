use std::{borrow::Cow, time::Duration};

use message_encoding::MessageEncoding;
use tracing::Instrument;

use crate::{state::DeterministicState, io::{SyncConnection, SyncIO}, message_io::{read_message, send_message}, recoverable_state::{RecovGenerationEnd, RecoverableState}};
use super::{HandshakeError, MessageHelper};
use crate::utils::LogHelper;
use super::messages::*;


pub struct HandshakeClient<I: SyncIO> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
}

pub struct ClientFreshConnect<I: SyncIO> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
}

pub struct ClientRecovering<I: SyncIO> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
    recovery: RecoveryAttempt,
    generations: Vec<RecovGenerationEnd>,
}

pub struct ClientServerWantsState<I: SyncIO> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
    recovery: RecoveryAttempt,
}

pub struct ClientWouldBeRelayed<I: SyncIO> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
    reconnect: Option<RecoveryAttempt>,
    address: I::Address,
}

pub enum ClientConnectionAccepted<I: SyncIO> {
    FreshConnection(ClientFreshConnect<I>),
    RecoveringConnection(ClientRecovering<I>),
    RequestingState(ClientServerWantsState<I>),
    RelayOption(ClientWouldBeRelayed<I>),
}

pub enum ClientConnectionAcceptedDirect<I: SyncIO> {
    FreshConnection(ClientFreshConnect<I>),
    RecoveringConnection(ClientRecovering<I>),
    RequestingState(ClientServerWantsState<I>),
}

#[derive(Clone)]
pub struct RecoveryAttempt {
    pub state_id: u64,
    pub state_generation: u64,
    pub state_sequence: u64,
}

impl<I: SyncIO> HandshakeClient<I> {
    pub fn new(conn: SyncConnection<I>) -> Self {
        HandshakeClient {
            buffer: Vec::with_capacity(1024),
            conn,
        }
    }

    pub async fn connect(self) -> Result<ClientConnectionAccepted<I>, HandshakeError<I::Address>> {
        self.reconnect_opt(None).await
    }

    pub async fn reconnect(self, recover: RecoveryAttempt) -> Result<ClientConnectionAccepted<I>, HandshakeError<I::Address>> {
        self.reconnect_opt(Some(recover)).await
    }

    pub async fn reconnect_opt(mut self, reconnect: Option<RecoveryAttempt>) -> Result<ClientConnectionAccepted<I>, HandshakeError<I::Address>> {
        let span = tracing::info_span!("HandshakeClient::connect", reconnect = reconnect.is_some());

        async {
            if let Some(recover) = reconnect.clone() {
                I::send(
                    &mut self.buffer,
                    &HandshakeMessage::ConnectRequest(ClientConnectRequest::Reconnect {
                        state_id: recover.state_id,
                        state_generation: recover.state_generation,
                        state_sequence: recover.state_sequence,
                    }),
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
                    ServerConnectResponse::RejectConnection => {}
                    ServerConnectResponse::AcceptConnection => return Err(HandshakeError::UnexpectedResponse(
                        HandshakeMessage::ConnectResponse(ServerConnectResponse::AcceptConnection)
                    )),
                    ServerConnectResponse::ProposeRelay { leader_address } => return Ok(ClientConnectionAccepted::RelayOption(ClientWouldBeRelayed {
                        buffer: self.buffer,
                        conn: self.conn,
                        reconnect,
                        address: leader_address,
                    })),
                    ServerConnectResponse::RequestState => return Ok(ClientConnectionAccepted::RequestingState(ClientServerWantsState {
                        buffer: self.buffer,
                        conn: self.conn,
                        recovery: recover,
                    })),
                    ServerConnectResponse::AcceptRecovery { generations } => return Ok(ClientConnectionAccepted::RecoveringConnection(ClientRecovering {
                        buffer: self.buffer,
                        conn: self.conn,
                        recovery: recover,
                        generations,
                    })),
                }
            }

            I::send(
                &mut self.buffer,
                &HandshakeMessage::ConnectRequest(ClientConnectRequest::Connect),
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
                ServerConnectResponse::ProposeRelay { leader_address } => Ok(ClientConnectionAccepted::RelayOption(ClientWouldBeRelayed {
                    buffer: self.buffer,
                    conn: self.conn,
                    reconnect,
                    address: leader_address,
                })),
                ServerConnectResponse::AcceptConnection => Ok(ClientConnectionAccepted::FreshConnection(ClientFreshConnect {
                    buffer: self.buffer,
                    conn: self.conn,
                })),
                other => Err(HandshakeError::UnexpectedResponse(HandshakeMessage::ConnectResponse(other))).log(),
            }
        }.instrument(span).await
    }
}

pub struct ClientConnected<'a, I: SyncIO, D: DeterministicState> {
    pub buffer: Vec<u8>,
    pub conn: SyncConnection<I>,
    pub state: Cow<'a, RecoverableState<D>>,
    pub remote_state: bool,
}

impl<'a, I: SyncIO, D: DeterministicState> ClientConnected<'a, I, D> {
    pub fn into_owned(self) -> ClientConnected<'static, I, D> {
        ClientConnected {
            buffer: self.buffer,
            conn: self.conn,
            state: Cow::Owned(self.state.into_owned()),
            remote_state: self.remote_state,
        }
    }
}

impl<I: SyncIO> ClientFreshConnect<I> {
    pub async fn load_state<D: DeterministicState + MessageEncoding>(mut self) -> Result<ClientConnected<'static, I, D>, HandshakeError<I::Address>> {
        let span = tracing::info_span!("ClientFreshConnect::load_state");

        async {
            let Some(state) = read_message::<RecoverableState<D>, _>(
                &mut self.buffer,
                &mut self.conn.read,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::ReadError).log()? else {
                return Err(HandshakeError::UnexpectedEmptyMessage).log();
            };

            Ok(ClientConnected {
                buffer: self.buffer,
                conn: self.conn,
                state: Cow::Owned(state),
                remote_state: true,
            })
        }.instrument(span).await
    }
}

impl<I: SyncIO> ClientRecovering<I> {
    pub fn recover<D: DeterministicState + Clone>(self, mut state: RecoverableState<D>) -> Result<ClientConnected<'static, I, D>, HandshakeError<I::Address>> {
        if state.id() != self.recovery.state_id || state.generation() != self.recovery.state_generation || state.sequence() != self.recovery.state_sequence {
            return Err(HandshakeError::InvalidStateIdOrSequence).log();
        }

        state.set_generations(self.generations)
            .map_err(HandshakeError::RecoverError)?;

        Ok(ClientConnected {
            buffer: self.buffer,
            conn: self.conn,
            state: Cow::Owned(state),
            remote_state: false,
        })
    }
}

impl<I: SyncIO> ClientServerWantsState<I> {
    pub async fn provide_state<'a, D: DeterministicState + MessageEncoding>(mut self, state: Cow<'a, RecoverableState<D>>) -> Result<ClientConnected<'a, I, D>, HandshakeError<I::Address>> {
        let span = tracing::info_span!("ClientServerWantsState::provide_state", id = state.id(), seq = state.sequence());

        async {
            if state.id() != self.recovery.state_id {
                return Err(HandshakeError::InvalidStateIdOrSequence).log();
            }

            send_message(
                &mut self.buffer,
                &state,
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError)?;

            let response = I::read_msg(
                &mut self.buffer,
                &mut self.conn.read,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::ReadError)?;

            let response = match response {
                Some(HandshakeMessage::ServerSnapshotResponse(response)) => response,
                None => return Err(HandshakeError::UnexpectedEmptyMessage).log(),
                Some(other) => return Err(HandshakeError::UnexpectedResponse(other)).log(),
            };

            match response {
                ServerSnapshotResponse::SnapshotAccept => Ok(ClientConnected {
                    buffer: self.buffer,
                    conn: self.conn,
                    state,
                    remote_state: false,
                }),
                ServerSnapshotResponse::SnapshotReject => {
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
                        ServerConnectResponse::RejectConnection => {
                            I::send(
                                &mut self.buffer,
                                &HandshakeMessage::ConnectRequest(ClientConnectRequest::Connect),
                                &mut self.conn.write,
                                Duration::from_secs(2),
                            ).await.log().map_err(HandshakeError::SendError)?;

                            let recv = I::read_msg(
                                &mut self.buffer,
                                &mut self.conn.read,
                                Duration::from_secs(2)
                            ).await
                                .map_err(HandshakeError::ReadError).log()?
                                .ok_or(HandshakeError::UnexpectedEmptyMessage).log()?;
                            
                            if !matches!(recv, HandshakeMessage::<I::Address>::ConnectResponse(ServerConnectResponse::AcceptConnection)) {
                                return Err(HandshakeError::UnexpectedResponse(recv)).log();
                            }

                            ClientFreshConnect {
                                buffer: self.buffer,
                                conn: self.conn,
                            }.load_state().await
                        }
                        ServerConnectResponse::AcceptRecovery { generations } => {
                            ClientRecovering {
                                buffer: self.buffer,
                                conn: self.conn,
                                recovery: self.recovery,
                                generations,
                            }.recover(state.into_owned())
                        }
                        other => Err(HandshakeError::UnexpectedResponse(HandshakeMessage::ConnectResponse(other))).log(),
                    }
                }
            }
        }.instrument(span).await
    }
}

impl<I: SyncIO> ClientWouldBeRelayed<I> {
    pub fn leader_address(&self) -> I::Address {
        self.address.clone()
    }

    pub async fn accept_relay(mut self) -> Result<ClientConnectionAcceptedDirect<I>, HandshakeError<I::Address>> {
        let span = tracing::info_span!("ClientWouldBeRelayed::accept_relay");

        async {
            I::send(
                &mut self.buffer,
                &HandshakeMessage::RelayFollowup(ClientRelayFollowup::AcceptRelay),
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError)?;

            let connect = HandshakeClient {
                buffer: self.buffer,
                conn: self.conn,
            }.reconnect_opt(self.reconnect).await?;

            connect.into_no_relay().map_err(|_| HandshakeError::RelayLoopDetected)
        }.instrument(span).await
    }

    pub async fn reject_relay(mut self) -> Result<I::Address, HandshakeError<I::Address>> {
        let span = tracing::info_span!("ClientWouldBeRelayed::reject_relay");

        async {
            I::send(
                &mut self.buffer,
                &HandshakeMessage::RelayFollowup(ClientRelayFollowup::RejectRelay),
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError)?;

            Ok(self.address)
        }.instrument(span).await
    }
}

impl<I: SyncIO> ClientConnectionAccepted<I> {
    pub fn into_no_relay(self) -> Result<ClientConnectionAcceptedDirect<I>, Self> {
        match self {
            ClientConnectionAccepted::RequestingState(a) => Ok(ClientConnectionAcceptedDirect::RequestingState(a)),
            ClientConnectionAccepted::FreshConnection(a) => Ok(ClientConnectionAcceptedDirect::FreshConnection(a)),
            ClientConnectionAccepted::RecoveringConnection(a) => Ok(ClientConnectionAcceptedDirect::RecoveringConnection(a)),
            other => Err(other),
        }
    }
}

