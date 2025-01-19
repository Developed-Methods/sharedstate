use message_encoding::MessageEncoding;

use crate::{message_io::unknown_id_err, recoverable_state::{RecovGenerationEnd, RecoverableStateDetails}};


#[derive(Debug, PartialEq, Eq)]
pub enum HandshakeMessage {
    ConnectRequest(ConnectRequest),
    ConnectResponse(ConnectResponse),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConnectRequest {
    Connect,
    Reconnect(RecoverableStateDetails),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConnectResponse {
    AcceptConnection,
    RejectConnection,
    AcceptRecovery(RecoverableStateDetails),
}

impl MessageEncoding for HandshakeMessage {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;

        match self {
            /* Request */
            Self::ConnectRequest(ConnectRequest::Connect) => {
                sum += 1u16.write_to(out)?;
            }
            Self::ConnectRequest(ConnectRequest::Reconnect(details)) => {
                sum += 2u16.write_to(out)?;
                sum += details.write_to(out)?;
            }
            /* Response */
            Self::ConnectResponse(ConnectResponse::AcceptConnection) => {
                sum += 3u16.write_to(out)?;
            }
            Self::ConnectResponse(ConnectResponse::RejectConnection) => {
                sum += 4u16.write_to(out)?;
            }
            Self::ConnectResponse(ConnectResponse::AcceptRecovery(details)) => {
                sum += 5u16.write_to(out)?;
                sum += details.write_to(out)?;
            }
        }

        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        let id = u16::read_from(read)?;

        match id {
            1 => Ok(Self::ConnectRequest(ConnectRequest::Connect)),
            2 => Ok(Self::ConnectRequest(ConnectRequest::Reconnect(MessageEncoding::read_from(read)?))),
            3 => Ok(Self::ConnectResponse(ConnectResponse::AcceptConnection)),
            4 => Ok(Self::ConnectResponse(ConnectResponse::RejectConnection)),
            5 => Ok(Self::ConnectResponse(ConnectResponse::AcceptRecovery(MessageEncoding::read_from(read)?))),
            id => Err(unknown_id_err(id, "invalid id from HandshakeMessage")),
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::VecDeque;

    use message_encoding::test_assert_valid_encoding;
    use crate::recoverable_state::RecovGenerationEnd;
    use super::*;

    #[test]
    fn handshake_msg_tests() {
        let history: VecDeque<_> = [
            RecovGenerationEnd { generation: 10, next_sequence: 100 },
            RecovGenerationEnd { generation: 11, next_sequence: 100 },
            RecovGenerationEnd { generation: 12, next_sequence: 102 },
        ].into_iter().collect();

        test_assert_valid_encoding(HandshakeMessage::ConnectRequest(ConnectRequest::Connect));
        test_assert_valid_encoding(HandshakeMessage::ConnectRequest(ConnectRequest::Reconnect(RecoverableStateDetails { sequence: 100, id: 20, generation: 3, state_sequence: 9, history: history.clone() })));
        test_assert_valid_encoding(HandshakeMessage::ConnectResponse(ConnectResponse::AcceptConnection));
        test_assert_valid_encoding(HandshakeMessage::ConnectResponse(ConnectResponse::RejectConnection));
        test_assert_valid_encoding(HandshakeMessage::ConnectResponse(ConnectResponse::AcceptRecovery(RecoverableStateDetails { sequence: 100, id: 20, generation: 3, state_sequence: 9, history: history.clone() })));
    }
}
