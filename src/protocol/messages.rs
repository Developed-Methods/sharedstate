use std::fmt::Debug;

use message_encoding::MessageEncoding;

use crate::{
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails},
    },
    utils::unknown_id_err,
};

pub const PROTOCOL_VERSION: u64 = 1;

pub enum SyncRequest<D: DeterministicState> {
    ProtocolVersion(u64),
    Subscribe(RecoverableStateDetails),
    Action {
        state_id: u64,
        action: RecoverableStateAction<D::Action>,
    },
}

impl<D: DeterministicState> Debug for SyncRequest<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProtocolVersion(v) => write!(f, "ProtocolVersion({v})"),
            Self::Subscribe(details) => write!(f, "Subscribe({details:?})"),
            Self::Action { state_id, .. } => write!(f, "Action(state_id: {state_id})"),
        }
    }
}

pub enum SyncResponse<D: DeterministicState> {
    Ok,
    NotConnected,
    FreshState(RecoverableState<D>),
    Action {
        seq: u64,
        action: RecoverableStateAction<D::AuthorityAction>,
    },
}

impl<D: DeterministicState> SyncResponse<D> {
    pub fn name(&self) -> &'static str {
        match self {
            SyncResponse::Ok => "Ok",
            SyncResponse::NotConnected => "NotConnected",
            SyncResponse::FreshState(_) => "FreshState",
            SyncResponse::Action { .. } => "Action",
        }
    }
}

impl<D: DeterministicState> MessageEncoding for SyncRequest<D>
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
            Self::Action { state_id, action } => {
                sum += 2u16.write_to(out)?;
                sum += state_id.write_to(out)?;
                action.write_to(out)?
            }
        };

        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(match u16::read_from(read)? {
            0 => Self::ProtocolVersion(MessageEncoding::read_from(read)?),
            1 => Self::Subscribe(MessageEncoding::read_from(read)?),
            2 => Self::Action {
                state_id: MessageEncoding::read_from(read)?,
                action: MessageEncoding::read_from(read)?,
            },
            other => return Err(unknown_id_err(other, "SyncRequest")),
        })
    }
}

impl<D: DeterministicState> MessageEncoding for SyncResponse<D>
where
    D::AuthorityAction: MessageEncoding,
    D: MessageEncoding,
{
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += match self {
            Self::Ok => 0u16.write_to(out)?,
            Self::NotConnected => 1u16.write_to(out)?,
            Self::FreshState(state) => {
                sum += 2u16.write_to(out)?;
                state.write_to(out)?
            }
            Self::Action { seq, action } => {
                sum += 3u16.write_to(out)?;
                sum += seq.write_to(out)?;
                action.write_to(out)?
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
            other => return Err(unknown_id_err(other, "SyncResponse")),
        })
    }
}
