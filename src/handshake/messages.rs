use message_encoding::MessageEncoding;

use crate::{message_io::unknown_id_err, recoverable_state::RecovGenerationEnd};


#[derive(Debug, PartialEq, Eq)]
pub enum HandshakeMessage<A: MessageEncoding> {
    ConnectRequest(ClientConnectRequest),
    ConnectResponse(ServerConnectResponse<A>),
    ServerSnapshotResponse(ServerSnapshotResponse),
    RelayFollowup(ClientRelayFollowup),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ClientConnectRequest {
    Connect,
    Reconnect {
        state_id: u64,
        state_generation: u64,
        state_sequence: u64
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum ServerConnectResponse<A: MessageEncoding> {
    AcceptConnection,
    AcceptRecovery { generations: Vec<RecovGenerationEnd> },
    RejectConnection,
    ProposeRelay { leader_address: A },
    RequestState,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ServerSnapshotResponse {
    SnapshotAccept,
    SnapshotReject,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ClientRelayFollowup {
    AcceptRelay,
    RejectRelay,
}

impl<A: MessageEncoding> MessageEncoding for HandshakeMessage<A> {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;

        match self {
            /* Request */
            Self::ConnectRequest(ClientConnectRequest::Connect) => {
                sum += 1u16.write_to(out)?;
            }
            Self::ConnectRequest(ClientConnectRequest::Reconnect { state_id, state_generation, state_sequence }) => {
                sum += 2u16.write_to(out)?;
                sum += state_id.write_to(out)?;
                sum += state_generation.write_to(out)?;
                sum += state_sequence.write_to(out)?;
            }
            /* Response */
            Self::ConnectResponse(ServerConnectResponse::AcceptConnection) => {
                sum += 3u16.write_to(out)?;
            }
            Self::ConnectResponse(ServerConnectResponse::RejectConnection) => {
                sum += 4u16.write_to(out)?;
            }
            Self::ConnectResponse(ServerConnectResponse::ProposeRelay { leader_address }) => {
                sum += 5u16.write_to(out)?;
                sum += leader_address.write_to(out)?;
            }
            Self::ConnectResponse(ServerConnectResponse::RequestState) => {
                sum += 6u16.write_to(out)?;
            }
            Self::ConnectResponse(ServerConnectResponse::AcceptRecovery { generations }) => {
                sum += 7u16.write_to(out)?;
                sum += (generations.len() as u64).write_to(out)?;
                for gen in generations {
                    sum += gen.generation.write_to(out)?;
                    sum += gen.next_sequence.write_to(out)?;
                }
            }
            /* ServerSnapshotResponse */
            Self::ServerSnapshotResponse(ServerSnapshotResponse::SnapshotAccept) => {
                sum += 8u16.write_to(out)?;
            }
            Self::ServerSnapshotResponse(ServerSnapshotResponse::SnapshotReject) => {
                sum += 9u16.write_to(out)?;
            }
            /* RelayFollowup */
            Self::RelayFollowup(ClientRelayFollowup::AcceptRelay) => {
                sum += 10u16.write_to(out)?;
            }
            Self::RelayFollowup(ClientRelayFollowup::RejectRelay) => {
                sum += 11u16.write_to(out)?;
            }
        }

        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        let id = u16::read_from(read)?;

        match id {
            1 => Ok(Self::ConnectRequest(ClientConnectRequest::Connect)),
            2 => Ok(Self::ConnectRequest(ClientConnectRequest::Reconnect {
                state_id: MessageEncoding::read_from(read)?,
                state_generation: MessageEncoding::read_from(read)?,
                state_sequence: MessageEncoding::read_from(read)?,
            })),
            3 => Ok(Self::ConnectResponse(ServerConnectResponse::AcceptConnection)),
            4 => Ok(Self::ConnectResponse(ServerConnectResponse::RejectConnection)),
            5 => Ok(Self::ConnectResponse(ServerConnectResponse::ProposeRelay {
                leader_address: MessageEncoding::read_from(read)?,
            })),
            6 => Ok(Self::ConnectResponse(ServerConnectResponse::RequestState)),
            7 => Ok(Self::ConnectResponse(ServerConnectResponse::AcceptRecovery { generations: {
                let count = u64::read_from(read)? as usize;
                let mut gens = Vec::with_capacity(count);
                for _ in 0..count {
                    gens.push(RecovGenerationEnd {
                        generation: MessageEncoding::read_from(read)?,
                        next_sequence: MessageEncoding::read_from(read)?,
                    });
                }
                gens
            }})),
            8 => Ok(Self::ServerSnapshotResponse(ServerSnapshotResponse::SnapshotAccept)),
            9 => Ok(Self::ServerSnapshotResponse(ServerSnapshotResponse::SnapshotReject)),
            10 => Ok(Self::RelayFollowup(ClientRelayFollowup::AcceptRelay)),
            11 => Ok(Self::RelayFollowup(ClientRelayFollowup::RejectRelay)),
            id => Err(unknown_id_err(id, "invalid id from HandshakeMessage")),
        }
    }
}

#[cfg(test)]
mod test {
    use message_encoding::test_assert_valid_encoding;
    use crate::recoverable_state::RecovGenerationEnd;
    use super::*;

    #[test]
    fn handshake_msg_tests() {
        test_assert_valid_encoding(HandshakeMessage::<i32>::ConnectRequest(ClientConnectRequest::Connect));
        test_assert_valid_encoding(HandshakeMessage::<i32>::ConnectRequest(ClientConnectRequest::Reconnect { state_id: 1, state_generation: 2, state_sequence: 3 }));
        test_assert_valid_encoding(HandshakeMessage::<i32>::ConnectResponse(ServerConnectResponse::RequestState));
        test_assert_valid_encoding(HandshakeMessage::<i32>::ConnectResponse(ServerConnectResponse::AcceptConnection));
        test_assert_valid_encoding(HandshakeMessage::<i32>::ConnectResponse(ServerConnectResponse::RejectConnection));
        test_assert_valid_encoding(HandshakeMessage::<i32>::ConnectResponse(ServerConnectResponse::ProposeRelay { leader_address: 100 }));
        test_assert_valid_encoding(HandshakeMessage::<i32>::ConnectResponse(ServerConnectResponse::AcceptRecovery { generations: vec![
            RecovGenerationEnd { generation: 10, next_sequence: 100 },
            RecovGenerationEnd { generation: 11, next_sequence: 100 },
            RecovGenerationEnd { generation: 12, next_sequence: 102 },
        ] }));
        test_assert_valid_encoding(HandshakeMessage::<i32>::ServerSnapshotResponse(ServerSnapshotResponse::SnapshotAccept));
        test_assert_valid_encoding(HandshakeMessage::<i32>::ServerSnapshotResponse(ServerSnapshotResponse::SnapshotReject));
        test_assert_valid_encoding(HandshakeMessage::<i32>::RelayFollowup(ClientRelayFollowup::AcceptRelay));
        test_assert_valid_encoding(HandshakeMessage::<i32>::RelayFollowup(ClientRelayFollowup::RejectRelay));
    }
}
