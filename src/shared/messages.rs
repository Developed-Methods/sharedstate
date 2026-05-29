use std::{fmt::Debug, num::NonZeroU64};

use message_encoding::MessageEncoding;

use crate::{
    net::sync_io::SyncIOAddress,
    state::{
        determinstic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails},
    },
    utils::{unknown_id_err, unknown_version_err},
};

pub enum SyncRequest<A: SyncIOAddress, D: DeterministicState> {
    ProtocolVersion(u64),
    MyAddress(A),
    Ping(u64),
    SubscribeFresh,
    ShareLeaderPath,
    SharePeers(Vec<SharePeerDetails<A>>),
    SubscribeRecovery(RecoverableStateDetails),
    Action { source: A, action: D::Action },
    LeaderStatus { address: A, status: LeaderStatus },
}

#[derive(Debug)]
pub enum LeaderStatus {
    Promoted,
    Offline,
    Voted,
}

#[derive(Debug)]
pub struct SharePeerDetails<A: SyncIOAddress> {
    pub address: A,
    pub can_be_leader: Option<bool>,
    pub last_global_activity: Option<NonZeroU64>,
}

impl<A: SyncIOAddress> From<A> for SharePeerDetails<A> {
    fn from(value: A) -> Self {
        SharePeerDetails {
            address: value,
            can_be_leader: None,
            last_global_activity: None,
        }
    }
}

impl<A: SyncIOAddress, D: DeterministicState> Debug for SyncRequest<A, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProtocolVersion(v) => write!(f, "ProtocolVersion({v})"),
            Self::MyAddress(address) => write!(f, "MyAddress({address:?})"),
            Self::Ping(num) => write!(f, "Ping({num})"),
            Self::SubscribeFresh => write!(f, "SubscribeFresh"),
            Self::ShareLeaderPath => write!(f, "ShareLeaderPath"),
            Self::SharePeers(peers) => write!(f, "NoticePeers({peers:?})"),
            Self::SubscribeRecovery(details) => write!(f, "SubscribeRecovery({details:?})"),
            Self::Action { source, .. } => write!(f, "Action(source: {source:?})"),
            Self::LeaderStatus { address, status } => {
                write!(f, "LeaderStatus({address:?} {status:?})")
            }
        }
    }
}

pub enum SyncResponse<A: SyncIOAddress, D: DeterministicState> {
    Pong(u64),
    Accepted(u64),
    RecoveryFailed(RecoverableStateDetails),
    FreshState(RecoverableState<D>),
    AuthorityAction(u64, RecoverableStateAction<D::AuthorityAction>),
    LeaderPath(Vec<A>),
    NoPathToLeader,
    Peers(Vec<SharePeerDetails<A>>),
    Ok,
    ActionStreamClosed,
}

impl<A: SyncIOAddress, D: DeterministicState> MessageEncoding for SyncRequest<A, D>
where
    D::Action: MessageEncoding,
{
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += match self {
            Self::MyAddress(addr) => {
                sum += 0u16.write_to(out)?;
                addr.write_to(out)?
            }
            Self::Ping(num) => {
                sum += 1u16.write_to(out)?;
                num.write_to(out)?
            }
            Self::SubscribeFresh => 3u16.write_to(out)?,
            Self::ShareLeaderPath => 4u16.write_to(out)?,
            Self::SharePeers(peers) => {
                sum += 6u16.write_to(out)?;
                write_vec(peers, out)?
            }
            Self::SubscribeRecovery(details) => {
                sum += 7u16.write_to(out)?;
                details.write_to(out)?
            }
            Self::Action { source, action } => {
                sum += 8u16.write_to(out)?;
                sum += source.write_to(out)?;
                action.write_to(out)?
            }
            Self::ProtocolVersion(version) => {
                sum += 9u16.write_to(out)?;
                version.write_to(out)?
            }
            Self::LeaderStatus { address, status } => {
                sum += 10u16.write_to(out)?;
                sum += address.write_to(out)?;
                status.write_to(out)?
            }
        };

        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(match u16::read_from(read)? {
            0 => Self::MyAddress(MessageEncoding::read_from(read)?),
            1 => Self::Ping(MessageEncoding::read_from(read)?),
            3 => Self::SubscribeFresh,
            4 => Self::ShareLeaderPath,
            6 => Self::SharePeers(read_vec(read)?),
            7 => Self::SubscribeRecovery(MessageEncoding::read_from(read)?),
            8 => Self::Action {
                source: MessageEncoding::read_from(read)?,
                action: MessageEncoding::read_from(read)?,
            },
            9 => Self::ProtocolVersion(MessageEncoding::read_from(read)?),
            10 => Self::LeaderStatus {
                address: MessageEncoding::read_from(read)?,
                status: MessageEncoding::read_from(read)?,
            },
            other => return Err(unknown_id_err(other, "SyncRequest")),
        })
    }
}

impl MessageEncoding for LeaderStatus {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let num = match self {
            LeaderStatus::Offline => 0u16,
            LeaderStatus::Promoted => 1u16,
            LeaderStatus::Voted => 2u16,
        };
        num.write_to(out)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        match u16::read_from(read)? {
            0 => Ok(LeaderStatus::Offline),
            1 => Ok(LeaderStatus::Promoted),
            2 => Ok(LeaderStatus::Voted),
            id => Err(unknown_id_err(id, "LeaderStatus")),
        }
    }
}

impl<A> MessageEncoding for SharePeerDetails<A>
where
    A: SyncIOAddress,
{
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += 1u16.write_to(out)?;
        sum += self.address.write_to(out)?;
        sum += self.can_be_leader.write_to(out)?;
        sum += self
            .last_global_activity
            .map(|v| v.get())
            .unwrap_or(0)
            .write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        let version = u16::read_from(read)?;
        if version != 1 {
            return Err(unknown_version_err(version, "SharePeerDetails"));
        }

        Ok(Self {
            address: MessageEncoding::read_from(read)?,
            can_be_leader: MessageEncoding::read_from(read)?,
            last_global_activity: NonZeroU64::new(u64::read_from(read)?),
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
            Self::Pong(num) => {
                sum += 1u16.write_to(out)?;
                num.write_to(out)?
            }
            Self::Accepted(next_seq) => {
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
            Self::Ok => 7u16.write_to(out)?,
            Self::ActionStreamClosed => 8u16.write_to(out)?,
            Self::RecoveryFailed(seq) => {
                sum += 9u16.write_to(out)?;
                seq.write_to(out)?
            }
            Self::NoPathToLeader => 10u16.write_to(out)?,
        };

        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(match u16::read_from(read)? {
            1 => Self::Pong(MessageEncoding::read_from(read)?),
            2 => Self::Accepted(MessageEncoding::read_from(read)?),
            3 => Self::FreshState(MessageEncoding::read_from(read)?),
            4 => Self::AuthorityAction(
                MessageEncoding::read_from(read)?,
                MessageEncoding::read_from(read)?,
            ),
            5 => Self::LeaderPath(read_vec(read)?),
            6 => Self::Peers(read_vec(read)?),
            7 => Self::Ok,
            8 => Self::ActionStreamClosed,
            9 => Self::RecoveryFailed(MessageEncoding::read_from(read)?),
            10 => Self::NoPathToLeader,
            other => return Err(unknown_id_err(other, "SyncResponse")),
        })
    }
}

fn write_vec<T: MessageEncoding, W: std::io::Write>(
    v: &[T],
    out: &mut W,
) -> std::io::Result<usize> {
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
