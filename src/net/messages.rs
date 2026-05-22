use std::fmt::Debug;

use message_encoding::MessageEncoding;

use crate::{
    recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails},
    state::DeterministicState,
};

use super::{io::SyncIO, message_io::unknown_id_err};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerInfo<A> {
    pub address: A,
    pub is_sentinel: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SentinelConnectivity<A> {
    pub sentinel: A,
    pub is_connected: bool,
    pub disconnects_in_past_hour: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SentinelVote<A> {
    pub observed_master: A,
    pub observed_state_id: u64,
    pub candidate: A,
    pub candidate_state_accept_seq: u64,
    pub candidate_recover_accept_seq: u64,
}

pub enum SyncRequest<I: SyncIO, D: DeterministicState> {
    MyAddress(I::Address),
    NodeInfo {
        is_sentinel: bool,
    },
    Ping(u64),
    SubscribeFresh,
    ShareLeaderPath,
    SendMePeers,
    SendMePeerInfo,
    NoticePeers(Vec<I::Address>),
    NoticePeerInfo(Vec<PeerInfo<I::Address>>),
    SentinelConnectivityReport(Vec<SentinelConnectivity<I::Address>>),
    SentinelVote(SentinelVote<I::Address>),
    SubscribeRecovery(RecoverableStateDetails),
    Action {
        source: I::Address,
        action: D::Action,
    },
}

impl<I: SyncIO, D: DeterministicState> Debug for SyncRequest<I, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MyAddress(address) => write!(f, "MyAddress({address:?})"),
            Self::NodeInfo { is_sentinel } => write!(f, "NodeInfo({is_sentinel})"),
            Self::Ping(num) => write!(f, "Ping({})", num),
            Self::SubscribeFresh => write!(f, "SubscribeFresh"),
            Self::ShareLeaderPath => write!(f, "ShareLeaderPath"),
            Self::SendMePeers => write!(f, "SendMePeers"),
            Self::SendMePeerInfo => write!(f, "SendMePeerInfo"),
            Self::NoticePeers(peers) => write!(f, "NoticePeers({:?})", peers),
            Self::NoticePeerInfo(peers) => write!(f, "NoticePeerInfo({:?})", peers),
            Self::SentinelConnectivityReport(report) => {
                write!(f, "SentinelConnectivityReport({:?})", report)
            }
            Self::SentinelVote(vote) => write!(f, "SentinelVote({:?})", vote),
            Self::SubscribeRecovery(details) => write!(f, "SubscribeRecovery({:?})", details),
            Self::Action { source, .. } => write!(f, "Action(source: {:?})", source),
        }
    }
}

pub enum SyncResponse<I: SyncIO, D: DeterministicState> {
    NodeInfo { is_sentinel: bool },
    Pong(u64),
    RecoveryAccepted(u64),
    FreshState(RecoverableState<I::Address, D>),
    AuthorityAction(u64, RecoverableStateAction<I::Address, D::AuthorityAction>),
    LeaderPath(Vec<I::Address>),
    Peers(Vec<I::Address>),
    PeerInfo(Vec<PeerInfo<I::Address>>),
}

impl<I: SyncIO, D: DeterministicState> MessageEncoding for SyncRequest<I, D>
where
    I::Address: MessageEncoding,
    D::Action: MessageEncoding,
{
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += match self {
            Self::MyAddress(addr) => {
                sum += 0u16.write_to(out)?;
                addr.write_to(out)?
            }
            Self::NodeInfo { is_sentinel } => {
                sum += 2u16.write_to(out)?;
                write_bool(*is_sentinel, out)?
            }
            Self::Ping(num) => {
                sum += 1u16.write_to(out)?;
                num.write_to(out)?
            }
            Self::SubscribeFresh => 3u16.write_to(out)?,
            Self::ShareLeaderPath => 4u16.write_to(out)?,
            Self::SendMePeers => 5u16.write_to(out)?,
            Self::SendMePeerInfo => 9u16.write_to(out)?,
            Self::NoticePeers(peers) => {
                sum += 6u16.write_to(out)?;
                write_vec(peers, out)?
            }
            Self::NoticePeerInfo(peers) => {
                sum += 10u16.write_to(out)?;
                write_vec(peers, out)?
            }
            Self::SentinelConnectivityReport(report) => {
                sum += 11u16.write_to(out)?;
                write_vec(report, out)?
            }
            Self::SentinelVote(vote) => {
                sum += 12u16.write_to(out)?;
                vote.write_to(out)?
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
        };

        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(match u16::read_from(read)? {
            0 => Self::MyAddress(MessageEncoding::read_from(read)?),
            1 => Self::Ping(MessageEncoding::read_from(read)?),
            2 => Self::NodeInfo {
                is_sentinel: read_bool(read)?,
            },
            3 => Self::SubscribeFresh,
            4 => Self::ShareLeaderPath,
            5 => Self::SendMePeers,
            6 => Self::NoticePeers(read_vec(read)?),
            7 => Self::SubscribeRecovery(MessageEncoding::read_from(read)?),
            8 => Self::Action {
                source: MessageEncoding::read_from(read)?,
                action: MessageEncoding::read_from(read)?,
            },
            9 => Self::SendMePeerInfo,
            10 => Self::NoticePeerInfo(read_vec(read)?),
            11 => Self::SentinelConnectivityReport(read_vec(read)?),
            12 => Self::SentinelVote(MessageEncoding::read_from(read)?),
            other => return Err(unknown_id_err(other, "SyncRequest")),
        })
    }
}

impl<I: SyncIO, D: DeterministicState> MessageEncoding for SyncResponse<I, D>
where
    I::Address: MessageEncoding,
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
            Self::NodeInfo { is_sentinel } => {
                sum += 8u16.write_to(out)?;
                write_bool(*is_sentinel, out)?
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
            Self::PeerInfo(peers) => {
                sum += 7u16.write_to(out)?;
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
            4 => Self::AuthorityAction(
                MessageEncoding::read_from(read)?,
                MessageEncoding::read_from(read)?,
            ),
            5 => Self::LeaderPath(read_vec(read)?),
            6 => Self::Peers(read_vec(read)?),
            7 => Self::PeerInfo(read_vec(read)?),
            8 => Self::NodeInfo {
                is_sentinel: read_bool(read)?,
            },
            other => return Err(unknown_id_err(other, "SyncResponse")),
        })
    }
}

impl<A: MessageEncoding> MessageEncoding for PeerInfo<A> {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        Ok(self.address.write_to(out)? + write_bool(self.is_sentinel, out)?)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(PeerInfo {
            address: MessageEncoding::read_from(read)?,
            is_sentinel: read_bool(read)?,
        })
    }
}

impl<A: MessageEncoding> MessageEncoding for SentinelConnectivity<A> {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += self.sentinel.write_to(out)?;
        sum += write_bool(self.is_connected, out)?;
        sum += (self.disconnects_in_past_hour as u64).write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(SentinelConnectivity {
            sentinel: MessageEncoding::read_from(read)?,
            is_connected: read_bool(read)?,
            disconnects_in_past_hour: u64::read_from(read)? as u32,
        })
    }
}

impl<A: MessageEncoding> MessageEncoding for SentinelVote<A> {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += self.observed_master.write_to(out)?;
        sum += self.observed_state_id.write_to(out)?;
        sum += self.candidate.write_to(out)?;
        sum += self.candidate_state_accept_seq.write_to(out)?;
        sum += self.candidate_recover_accept_seq.write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(SentinelVote {
            observed_master: MessageEncoding::read_from(read)?,
            observed_state_id: MessageEncoding::read_from(read)?,
            candidate: MessageEncoding::read_from(read)?,
            candidate_state_accept_seq: MessageEncoding::read_from(read)?,
            candidate_recover_accept_seq: MessageEncoding::read_from(read)?,
        })
    }
}

fn write_bool<T: std::io::Write>(value: bool, out: &mut T) -> std::io::Result<usize> {
    (if value { 1u16 } else { 0u16 }).write_to(out)
}

fn read_bool<T: std::io::Read>(read: &mut T) -> std::io::Result<bool> {
    match u16::read_from(read)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(unknown_id_err(other, "bool")),
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
