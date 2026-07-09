use std::fmt::Debug;

use message_encoding::MessageEncoding;

use crate::{
    cluster::node_state::DebugInfo,
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails},
    },
    transport::traits::SyncIOAddress,
    utils::unknown_id_err,
};

pub const PROTOCOL_VERSION: u64 = 1;

pub enum SyncRequest<A: SyncIOAddress, D: DeterministicState> {
    ProtocolVersion(u64),
    Subscribe(RecoverableStateDetails),
    Action(D::Action),
    Ping(u64),
    GetNodeStatus,
    SetLeader(A),
    SetAvailablePeers(Vec<A>),
    GetCurrentLeader,
    GetCurrentStateRecoverDetails,
}

impl<A: SyncIOAddress, D: DeterministicState> Debug for SyncRequest<A, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProtocolVersion(v) => write!(f, "ProtocolVersion({v})"),
            Self::Subscribe(details) => write!(f, "Subscribe({details:?})"),
            Self::Action(..) => write!(f, "Action"),
            Self::Ping(id) => write!(f, "Ping({id})"),
            Self::GetNodeStatus => write!(f, "GetNodeStatus"),
            Self::SetLeader(leader) => write!(f, "SetLeader({leader:?})"),
            Self::SetAvailablePeers(peers) => write!(f, "SetAvailablePeers({peers:?})"),
            Self::GetCurrentLeader => write!(f, "GetCurrentLeader"),
            Self::GetCurrentStateRecoverDetails => write!(f, "GetCurrentStateRecoverDetails"),
        }
    }
}

pub enum SyncResponse<A: SyncIOAddress, D: DeterministicState> {
    Ok,
    NotConnected,
    Pong(u64),
    FreshState(RecoverableState<D>),
    Action {
        seq: u64,
        action: RecoverableStateAction<D::AuthorityAction>,
    },
    NodeStatus(DebugInfo<A>),
    CurrentLeader(A),
    CurrentStateRecoverDetails(RecoverableStateDetails),
}

impl<A: SyncIOAddress, D: DeterministicState> SyncResponse<A, D> {
    pub fn name(&self) -> &'static str {
        match self {
            SyncResponse::Ok => "Ok",
            SyncResponse::NotConnected => "NotConnected",
            SyncResponse::Pong(_) => "Pong",
            SyncResponse::FreshState(_) => "FreshState",
            SyncResponse::Action { .. } => "Action",
            SyncResponse::NodeStatus(_) => "NodeStatus",
            SyncResponse::CurrentLeader(_) => "CurrentLeader",
            SyncResponse::CurrentStateRecoverDetails(_) => "CurrentStateRecoverDetails",
        }
    }
}

impl<A: SyncIOAddress, D: DeterministicState> MessageEncoding for SyncRequest<A, D>
where
    D::Action: MessageEncoding,
{
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += match self {
            Self::ProtocolVersion(version) => {
                sum += 0u16.write_to(out)?;
                version.write_to(out)?
            }
            Self::Subscribe(details) => {
                sum += 1u16.write_to(out)?;
                details.write_to(out)?
            }
            Self::Action(action) => {
                sum += 2u16.write_to(out)?;
                action.write_to(out)?
            }
            Self::Ping(id) => {
                sum += 3u16.write_to(out)?;
                id.write_to(out)?
            }
            Self::GetNodeStatus => 4u16.write_to(out)?,
            Self::SetLeader(leader) => {
                sum += 5u16.write_to(out)?;
                leader.write_to(out)?
            }
            Self::SetAvailablePeers(peers) => {
                sum += 6u16.write_to(out)?;
                sum += (peers.len() as u64).write_to(out)?;
                for peer in peers {
                    sum += peer.write_to(out)?;
                }
                0
            }
            Self::GetCurrentLeader => 7u16.write_to(out)?,
            Self::GetCurrentStateRecoverDetails => 8u16.write_to(out)?,
        };

        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(match u16::read_from(read)? {
            0 => Self::ProtocolVersion(MessageEncoding::read_from(read)?),
            1 => Self::Subscribe(MessageEncoding::read_from(read)?),
            2 => Self::Action(MessageEncoding::read_from(read)?),
            3 => Self::Ping(MessageEncoding::read_from(read)?),
            4 => Self::GetNodeStatus,
            5 => Self::SetLeader(MessageEncoding::read_from(read)?),
            6 => {
                let len = u64::read_from(read)? as usize;
                let mut peers = Vec::with_capacity(len);
                for _ in 0..len {
                    peers.push(MessageEncoding::read_from(read)?);
                }
                Self::SetAvailablePeers(peers)
            }
            7 => Self::GetCurrentLeader,
            8 => Self::GetCurrentStateRecoverDetails,
            other => return Err(unknown_id_err(other, "SyncRequest")),
        })
    }
}

impl<A: SyncIOAddress, D: DeterministicState> MessageEncoding for SyncResponse<A, D>
where
    D::AuthorityAction: MessageEncoding,
    D: MessageEncoding,
{
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += match self {
            Self::Ok => 0u16.write_to(out)?,
            Self::NotConnected => 1u16.write_to(out)?,
            Self::Pong(id) => {
                sum += 4u16.write_to(out)?;
                id.write_to(out)?
            }
            Self::FreshState(state) => {
                sum += 2u16.write_to(out)?;
                state.write_to(out)?
            }
            Self::Action { seq, action } => {
                sum += 3u16.write_to(out)?;
                sum += seq.write_to(out)?;
                action.write_to(out)?
            }
            Self::NodeStatus(status) => {
                sum += 5u16.write_to(out)?;
                status.write_to(out)?
            }
            Self::CurrentLeader(leader) => {
                sum += 6u16.write_to(out)?;
                leader.write_to(out)?
            }
            Self::CurrentStateRecoverDetails(details) => {
                sum += 7u16.write_to(out)?;
                details.write_to(out)?
            }
        };

        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(match u16::read_from(read)? {
            0 => Self::Ok,
            1 => Self::NotConnected,
            2 => Self::FreshState(MessageEncoding::read_from(read)?),
            3 => Self::Action {
                seq: MessageEncoding::read_from(read)?,
                action: MessageEncoding::read_from(read)?,
            },
            4 => Self::Pong(MessageEncoding::read_from(read)?),
            5 => Self::NodeStatus(MessageEncoding::read_from(read)?),
            6 => Self::CurrentLeader(MessageEncoding::read_from(read)?),
            7 => Self::CurrentStateRecoverDetails(MessageEncoding::read_from(read)?),
            other => return Err(unknown_id_err(other, "SyncResponse")),
        })
    }
}
