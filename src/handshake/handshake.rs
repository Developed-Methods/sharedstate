use std::{borrow::Cow, time::Duration};

use message_encoding::MessageEncoding;

use crate::{sync::message_io::{read_message, send_message, unknown_id_err, ReadMessageError}, DeterministicState};

use super::io::{SyncIO, SyncConnection};

#[derive(Debug)]
enum ClientConnectRequest {
    Connect,
    Reconnect { state_id: u64, state_sequence: u64 },
}

#[derive(Debug)]
enum ServerConnectResponse<A: MessageEncoding> {
    AcceptConnection,
    RejectConnection,
    ProposeRelay { leader_address: A },
    RequestState,
}

#[derive(Debug)]
enum ServerSnapshotResponse {
    SnapshotAccept,
    SnapshotReject,
}

#[derive(Debug)]
enum ClientRelayFollowup {
    AcceptRelay,
    RejectRelay,
}

#[derive(Debug)]
enum HandshakeMessage<A: MessageEncoding> {
    ConnectRequest(ClientConnectRequest),
    ConnectResponse(ServerConnectResponse<A>),
    ServerSnapshotResponse(ServerSnapshotResponse),
    RelayFollowup(ClientRelayFollowup),
}
