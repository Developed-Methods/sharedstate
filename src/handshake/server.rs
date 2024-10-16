use std::time::Duration;

use message_encoding::MessageEncoding;
use tracing::Instrument;

use crate::io::{SyncConnection, SyncIO};
use crate::message_io::{read_message, send_message};
use crate::state::DeterministicState;

use crate::recoverable_state::{RecovGenerationEnd, RecoverableState};
use crate::utils::LogHelper;
use super::{HandshakeError, MessageHelper};
use super::messages::*;

pub struct HandshakeServer<I: SyncIO> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
}

pub struct ServerNewClientFresh<I: SyncIO> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
}

pub struct ServerNewClientWithData<I: SyncIO> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
    state_id: u64,
    state_generation: u64,
    state_sequence: u64,
}

pub enum ServerNewClient<I: SyncIO> {
    Fresh(ServerNewClientFresh<I>),
    WithData(ServerNewClientWithData<I>),
}

impl<I: SyncIO> HandshakeServer<I> {
    pub fn new(conn: SyncConnection<I>) -> Self {
        HandshakeServer {
            conn,
            buffer: Vec::with_capacity(1024),
        }
    }

    pub async fn relay(mut self, leader: I::Address) -> Result<Option<ServerNewClient<I>>, HandshakeError<I::Address>> {
        let span = tracing::info_span!("HandshakeServer::relay");

        async {
            let Some(init) = I::read_msg(
                &mut self.buffer,
                &mut self.conn.read,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::ReadError).log()? else {
                return Err(HandshakeError::UnexpectedEmptyMessage).log();
            };

            /* ensure request is ConnectRequest */
            match init {
                HandshakeMessage::ConnectRequest(_) => {},
                other => return Err(HandshakeError::UnexpectedResponse(other)).log(),
            }

            I::send(
                &mut self.buffer,
                &HandshakeMessage::ConnectResponse(ServerConnectResponse::ProposeRelay { leader_address: leader }),
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError).log()?;

            let Some(resp) = I::read_msg(
                &mut self.buffer,
                &mut self.conn.read,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::ReadError).log()? else {
                return Err(HandshakeError::UnexpectedEmptyMessage).log();
            };

            let resp = match resp {
                HandshakeMessage::RelayFollowup(resp) => resp,
                other => return Err(HandshakeError::UnexpectedResponse(other)).log(),
            };

            match resp {
                ClientRelayFollowup::AcceptRelay => self.accept().await.log().map(Some),
                ClientRelayFollowup::RejectRelay => Ok(None),
            }
        }.instrument(span).await
    }

    pub async fn accept(mut self) -> Result<ServerNewClient<I>, HandshakeError<I::Address>> {
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
                ClientConnectRequest::Connect => Ok(ServerNewClient::Fresh(ServerNewClientFresh {
                    buffer: self.buffer,
                    conn: self.conn,
                })),
                ClientConnectRequest::Reconnect { state_id, state_generation, state_sequence } => Ok(ServerNewClient::WithData(ServerNewClientWithData {
                    buffer: self.buffer,
                    conn: self.conn,
                    state_id,
                    state_generation,
                    state_sequence,
                })),
            }
        }.instrument(span).await
    }
}

impl<I: SyncIO> ServerNewClientFresh<I> {
    pub fn remote(&self) -> &I::Address {
        &self.conn.remote
    }

    pub async fn send_state<D: DeterministicState + MessageEncoding>(mut self, state: &RecoverableState<D>) -> Result<ServerClientReady<I>, HandshakeError<I::Address>> {
        let span = tracing::info_span!("ServerNewClientFresh::send_state", id = state.id(), seq = state.sequence());

        async {
            I::send(
                &mut self.buffer,
                &HandshakeMessage::ConnectResponse(ServerConnectResponse::AcceptConnection),
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError).log()?;

            send_message(
                &mut self.buffer,
                state,
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError).log()?;

            Ok(ServerClientReady {
                buffer: self.buffer,
                conn: self.conn,
                state_id: state.id(),
                state_sequence: state.sequence(),
            })
        }.instrument(span).await
    }
}

impl<I: SyncIO> ServerNewClientWithData<I> {
    pub fn remote(&self) -> &I::Address {
        &self.conn.remote
    }

    pub fn state_id(&self) -> u64 {
        self.state_id
    }

    pub fn state_generation(&self) -> u64 {
        self.state_generation
    }

    pub fn state_sequence(&self) -> u64 {
        self.state_sequence
    }

    pub async fn request_state<D: DeterministicState + MessageEncoding>(mut self) -> Result<ServerClientReceivedState<I, D>, HandshakeError<I::Address>> {
        let span = tracing::info_span!("ServerNewClientWithData::request_state");

        async {
            I::send(
                &mut self.buffer,
                &HandshakeMessage::ConnectResponse(ServerConnectResponse::RequestState),
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError).log()?;

            let Some(state) = read_message::<RecoverableState<D>, _>(
                &mut self.buffer,
                &mut self.conn.read,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::ReadError).log()? else {
                return Err(HandshakeError::UnexpectedEmptyMessage).log();
            };

            Ok(ServerClientReceivedState {
                conn: self.conn,
                buffer: self.buffer,
                state,
            })
        }.instrument(span).await
    }

    pub async fn accept_recover(mut self, generations: Vec<RecovGenerationEnd>) -> Result<ServerClientReady<I>, HandshakeError<I::Address>> {
        let span = tracing::info_span!("ServerNewClientWithData::accept_client");

        async {
            I::send(
                &mut self.buffer,
                &HandshakeMessage::ConnectResponse(ServerConnectResponse::AcceptRecovery { generations }),
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError).log()?;

            Ok(ServerClientReady {
                buffer: self.buffer,
                conn: self.conn,
                state_id: self.state_id,
                state_sequence: self.state_sequence,
            })
        }.instrument(span).await
    }

    pub async fn accept_client_with_state<D: DeterministicState + MessageEncoding>(mut self, state: &RecoverableState<D>) -> Result<ServerClientReady<I>, HandshakeError<I::Address>> {
        let span = tracing::info_span!("ServerNewClientWithData::accept_client_with_state", id = state.id(), seq = state.sequence());

        async {
            I::send(
                &mut self.buffer,
                &HandshakeMessage::ConnectResponse(ServerConnectResponse::RejectConnection),
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError).log()?;

            let accept = HandshakeServer {
                conn: self.conn,
                buffer: self.buffer
            }.accept().await.log()?;

            match accept {
                ServerNewClient::WithData(state) => Err(HandshakeError::UnexpectedResponse(HandshakeMessage::ConnectRequest(ClientConnectRequest::Reconnect {
                    state_id: state.state_id,
                    state_generation: state.state_generation,
                    state_sequence: state.state_sequence,
                }))).log(),
                ServerNewClient::Fresh(client) => client.send_state(state).await,
            }
        }.instrument(span).await
    }
}

pub struct ServerClientReceivedState<I: SyncIO, S: DeterministicState> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
    state: RecoverableState<S>,
}

pub struct ServerClientWithState<I: SyncIO, S: DeterministicState> {
    buffer: Vec<u8>,
    conn: SyncConnection<I>,
    state: RecoverableState<S>,
}

impl<I: SyncIO, S: DeterministicState> ServerClientReceivedState<I, S> {
    pub fn state(&self) -> &RecoverableState<S> {
        &self.state
    }

    pub async fn accept(mut self) -> Result<ServerClientWithState<I, S>, HandshakeError<I::Address>> {
        let span = tracing::info_span!("ServerClientReceivedState::accept");

        async {
            I::send(
                &mut self.buffer,
                &HandshakeMessage::ServerSnapshotResponse(ServerSnapshotResponse::SnapshotAccept),
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError).log()?;

            Ok(ServerClientWithState {
                buffer: self.buffer,
                conn: self.conn,
                state: self.state,
            })
        }.instrument(span).await
    }

    pub async fn reject(mut self) -> Result<ServerNewClientWithData<I>, HandshakeError<I::Address>> {
        let span = tracing::info_span!("ServerClientReceivedState::reject");

        async {
            I::send(
                &mut self.buffer,
                &HandshakeMessage::ServerSnapshotResponse(ServerSnapshotResponse::SnapshotReject),
                &mut self.conn.write,
                Duration::from_secs(2)
            ).await.map_err(HandshakeError::SendError).log()?;

            let state_id = self.state().id();
            let state_sequence = self.state().sequence();
            let state_generation = self.state().generation();

            Ok(ServerNewClientWithData {
                conn: self.conn,
                buffer: self.buffer,
                state_id,
                state_generation,
                state_sequence,
            })
        }.instrument(span).await
    }
}

impl<I: SyncIO, S: DeterministicState> ServerClientWithState<I, S> {
    pub fn state(&self) -> &RecoverableState<S> {
        &self.state
    }

    pub fn unbundle(self) -> (ServerClientReady<I>, RecoverableState<S>) {
        let state_id = self.state.id();
        let state_sequence = self.state.sequence();

        (
            ServerClientReady {
                buffer: self.buffer,
                conn: self.conn,
                state_id,
                state_sequence,
            },
            self.state,
        )
    }
}

pub struct ServerClientReady<I: SyncIO> {
    pub buffer: Vec<u8>,
    pub conn: SyncConnection<I>,
    pub state_id: u64,
    pub state_sequence: u64,
}

