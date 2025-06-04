use std::fmt::Debug;

use message_encoding::MessageEncoding;

use crate::{recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails}, state::DeterministicState};

use super::{io::SyncIO, message_io::unknown_id_err};

pub enum SyncRequest<I: SyncIO, D: DeterministicState> {
    Ping(u64),
    SubscribeFresh,
    ShareLeaderPath,
    SendMePeers,
    NoticePeers(Vec<I::Address>),
    SubscribeRecovery(RecoverableStateDetails),
    Action { source: I::Address, action: D::Action },
}

impl<I: SyncIO, D: DeterministicState> Debug for SyncRequest<I, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ping(num) => write!(f, "Ping({})", num),
            Self::SubscribeFresh => write!(f, "SubscribeFresh"),
            Self::ShareLeaderPath => write!(f, "ShareLeaderPath"),
            Self::SendMePeers => write!(f, "SendMePeers"),
            Self::NoticePeers(peers) => write!(f, "NoticePeers({:?})", peers),
            Self::SubscribeRecovery(details) => write!(f, "SubscribeRecovery({:?})", details),
            Self::Action { source, .. } => write!(f, "Action(source: {:?})", source),
        }
    }
}

pub enum SyncResponse<I: SyncIO, D: DeterministicState> {
    Pong(u64),
    RecoveryAccepted(u64),
    FreshState(RecoverableState<I::Address, D>),
    AuthorityAction(u64, RecoverableStateAction<I::Address, D::AuthorityAction>),
    LeaderPath(Vec<I::Address>),
    Peers(Vec<I::Address>),
}

impl<I: SyncIO, D: DeterministicState> MessageEncoding for SyncRequest<I, D> where I::Address: MessageEncoding, D::Action: MessageEncoding {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += match self {
            Self::Ping(num) => {
                sum += 1u16.write_to(out)?;
                num.write_to(out)?
            }
            Self::SubscribeFresh => 3u16.write_to(out)?,
            Self::ShareLeaderPath => 4u16.write_to(out)?,
            Self::SendMePeers => 5u16.write_to(out)?,
            Self::NoticePeers(peers) => {
                sum += 6u16.write_to(out)?;
                write_vec(peers, out)?
            },
            Self::SubscribeRecovery(details) => {
                sum += 7u16.write_to(out)?;
                details.write_to(out)?
            }
            Self::Action { source, action } => {
                sum += 8u16.write_to(out)?;
                sum += source.write_to(out)?;
                action.write_to(out)?
            }
        };

        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(match u16::read_from(read)? {
            1 => Self::Ping(MessageEncoding::read_from(read)?),
            3 => Self::SubscribeFresh,
            4 => Self::ShareLeaderPath,
            5 => Self::SendMePeers,
            6 => Self::NoticePeers(read_vec(read)?),
            7 => Self::SubscribeRecovery(MessageEncoding::read_from(read)?),
            8 => Self::Action {
                source: MessageEncoding::read_from(read)?,
                action: MessageEncoding::read_from(read)?,
            },
            other => return Err(unknown_id_err(other, "SyncRequest")),
        })
    }
}

impl<I: SyncIO, D: DeterministicState> MessageEncoding for SyncResponse<I, D> where I::Address: MessageEncoding, D::AuthorityAction: MessageEncoding, D: MessageEncoding {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += match self {
            Self::Pong(num) => {
                sum += 1u16.write_to(out)?;
                num.write_to(out)?
            }
            Self::RecoveryAccepted(next_seq) => {
                sum += 2u16.write_to(out)?;
                next_seq.write_to(out)?
            }
            Self::FreshState(state) => {
                sum += 3u16.write_to(out)?;
                state.write_to(out)?
            }
            Self::AuthorityAction(seq, action) => {
                sum += 4u16.write_to(out)?;
                sum += seq.write_to(out)?;
                action.write_to(out)?
            }
            Self::LeaderPath(path) => {
                sum += 5u16.write_to(out)?;
                write_vec(path, out)?
            }
            Self::Peers(peers) => {
                sum += 6u16.write_to(out)?;
                write_vec(peers, out)?
            }
        };

        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(match u16::read_from(read)? {
            1 => Self::Pong(MessageEncoding::read_from(read)?),
            2 => Self::RecoveryAccepted(MessageEncoding::read_from(read)?),
            3 => Self::FreshState(MessageEncoding::read_from(read)?),
            4 => Self::AuthorityAction(MessageEncoding::read_from(read)?, MessageEncoding::read_from(read)?),
            5 => Self::LeaderPath(read_vec(read)?),
            6 => Self::Peers(read_vec(read)?),
            other => return Err(unknown_id_err(other, "SyncResponse")),
        })
    }
}

fn write_vec<T: MessageEncoding, W: std::io::Write>(v: &[T], out: &mut W) -> std::io::Result<usize> {
    let mut sum = (v.len() as u64).write_to(out)?;
    for i in v {
        sum += i.write_to(out)?;
    }
    Ok(sum)
}

fn read_vec<T: MessageEncoding, R: std::io::Read>(read: &mut R) -> std::io::Result<Vec<T>> {
    let count = u64::read_from(read)? as usize;
    let mut vec = Vec::with_capacity(count);
    for _ in 0..count {
        vec.push(MessageEncoding::read_from(read)?);
    }
    Ok(vec)
}
