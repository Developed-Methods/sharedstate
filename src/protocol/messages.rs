use std::{fmt::Debug, num::NonZeroU64};

use message_encoding::MessageEncoding;

use crate::{
    state::{
        deterministic::DeterministicState,
        recoverable::{RecoverableState, RecoverableStateAction, RecoverableStateDetails},
    },
    transport::traits::SyncIOAddress,
    utils::{unknown_id_err, unknown_version_err},
};

pub enum SyncRequest<A: SyncIOAddress, D: DeterministicState> {
    ProtocolVersion(u64),
    MyAddress(A),
    Ping(u64),
    SharePeers(Vec<SharePeerDetails<A>>),
    WhoIsLeader,
    ShareLeaderPath,
    ShareElection,
    SubscribeFresh,
    SubscribeRecovery(RecoverableStateDetails),
    Action { source: A, action: D::Action },
    LeaderStatus { address: A, status: LeaderStatus<A> },
}

#[derive(Clone, Debug)]
pub enum LeaderStatus<A: SyncIOAddress> {
    Promoted { term: u64, leader: A },
    Offline { term: u64, leader: A },
    Observation(ElectionObservation<A>),
}

#[derive(Clone, Debug)]
pub struct LeaderInfoMessage<A: SyncIOAddress> {
    pub leader: Option<A>,
    pub path: Option<Vec<A>>,
    pub term: u64,
}

#[derive(Clone, Debug)]
pub struct ElectionObservation<A: SyncIOAddress> {
    pub observer: A,
    pub term: u64,
    pub leader: Option<A>,
    pub leader_path: Option<Vec<A>>,
    pub can_lead: bool,
    pub reachable_can_lead: Vec<A>,
    pub state_accept_seq: u64,
}

#[derive(Clone, Debug)]
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
            Self::SharePeers(peers) => write!(f, "SharePeers({peers:?})"),
            Self::WhoIsLeader => write!(f, "WhoIsLeader"),
            Self::ShareLeaderPath => write!(f, "ShareLeaderPath"),
            Self::ShareElection => write!(f, "ShareElection"),
            Self::SubscribeFresh => write!(f, "SubscribeFresh"),
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
    Ok,
    Peers(Vec<SharePeerDetails<A>>),
    LeaderInfo(LeaderInfoMessage<A>),
    Election(ElectionObservation<A>),
    LeaderPath(Vec<A>),
    NoPathToLeader,
    Accepted(u64),
    RecoveryFailed(RecoverableStateDetails),
    FreshState(RecoverableState<D>),
    AuthorityAction(u64, RecoverableStateAction<D::AuthorityAction>),
    ActionStreamClosed,
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
            Self::MyAddress(addr) => {
                sum += 1u16.write_to(out)?;
                addr.write_to(out)?
            }
            Self::Ping(num) => {
                sum += 2u16.write_to(out)?;
                num.write_to(out)?
            }
            Self::SharePeers(peers) => {
                sum += 3u16.write_to(out)?;
                write_vec(peers, out)?
            }
            Self::WhoIsLeader => 4u16.write_to(out)?,
            Self::ShareLeaderPath => 5u16.write_to(out)?,
            Self::ShareElection => 6u16.write_to(out)?,
            Self::SubscribeFresh => 7u16.write_to(out)?,
            Self::SubscribeRecovery(details) => {
                sum += 8u16.write_to(out)?;
                details.write_to(out)?
            }
            Self::Action { source, action } => {
                sum += 9u16.write_to(out)?;
                sum += source.write_to(out)?;
                action.write_to(out)?
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
            0 => Self::ProtocolVersion(MessageEncoding::read_from(read)?),
            1 => Self::MyAddress(MessageEncoding::read_from(read)?),
            2 => Self::Ping(MessageEncoding::read_from(read)?),
            3 => Self::SharePeers(read_vec(read)?),
            4 => Self::WhoIsLeader,
            5 => Self::ShareLeaderPath,
            6 => Self::ShareElection,
            7 => Self::SubscribeFresh,
            8 => Self::SubscribeRecovery(MessageEncoding::read_from(read)?),
            9 => Self::Action {
                source: MessageEncoding::read_from(read)?,
                action: MessageEncoding::read_from(read)?,
            },
            10 => Self::LeaderStatus {
                address: MessageEncoding::read_from(read)?,
                status: MessageEncoding::read_from(read)?,
            },
            other => return Err(unknown_id_err(other, "SyncRequest")),
        })
    }
}

impl<A: SyncIOAddress> MessageEncoding for LeaderStatus<A> {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += match self {
            LeaderStatus::Offline { term, leader } => {
                sum += 0u16.write_to(out)?;
                sum += term.write_to(out)?;
                leader.write_to(out)?
            }
            LeaderStatus::Promoted { term, leader } => {
                sum += 1u16.write_to(out)?;
                sum += term.write_to(out)?;
                leader.write_to(out)?
            }
            LeaderStatus::Observation(observation) => {
                sum += 2u16.write_to(out)?;
                observation.write_to(out)?
            }
        };
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(match u16::read_from(read)? {
            0 => Self::Offline {
                term: MessageEncoding::read_from(read)?,
                leader: MessageEncoding::read_from(read)?,
            },
            1 => Self::Promoted {
                term: MessageEncoding::read_from(read)?,
                leader: MessageEncoding::read_from(read)?,
            },
            2 => Self::Observation(MessageEncoding::read_from(read)?),
            id => return Err(unknown_id_err(id, "LeaderStatus")),
        })
    }
}

impl<A: SyncIOAddress> MessageEncoding for LeaderInfoMessage<A> {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += 1u16.write_to(out)?;
        sum += self.leader.write_to(out)?;
        sum += write_opt_vec(&self.path, out)?;
        sum += self.term.write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        let version = u16::read_from(read)?;
        if version != 1 {
            return Err(unknown_version_err(version, "LeaderInfoMessage"));
        }

        Ok(Self {
            leader: MessageEncoding::read_from(read)?,
            path: read_opt_vec(read)?,
            term: MessageEncoding::read_from(read)?,
        })
    }
}

impl<A: SyncIOAddress> MessageEncoding for ElectionObservation<A> {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += 2u16.write_to(out)?;
        sum += self.observer.write_to(out)?;
        sum += self.term.write_to(out)?;
        sum += self.leader.write_to(out)?;
        sum += write_opt_vec(&self.leader_path, out)?;
        sum += self.can_lead.write_to(out)?;
        sum += write_vec(&self.reachable_can_lead, out)?;
        sum += self.state_accept_seq.write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        let version = u16::read_from(read)?;
        if version != 2 {
            return Err(unknown_version_err(version, "ElectionObservation"));
        }

        Ok(Self {
            observer: MessageEncoding::read_from(read)?,
            term: MessageEncoding::read_from(read)?,
            leader: MessageEncoding::read_from(read)?,
            leader_path: read_opt_vec(read)?,
            can_lead: MessageEncoding::read_from(read)?,
            reachable_can_lead: read_vec(read)?,
            state_accept_seq: MessageEncoding::read_from(read)?,
        })
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
        sum += self.last_global_activity.map(|v| v.get()).unwrap_or(0).write_to(out)?;
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
            Self::LeaderInfo(info) => {
                sum += 11u16.write_to(out)?;
                info.write_to(out)?
            }
            Self::Election(observation) => {
                sum += 12u16.write_to(out)?;
                observation.write_to(out)?
            }
        };

        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(match u16::read_from(read)? {
            1 => Self::Pong(MessageEncoding::read_from(read)?),
            2 => Self::Accepted(MessageEncoding::read_from(read)?),
            3 => Self::FreshState(MessageEncoding::read_from(read)?),
            4 => Self::AuthorityAction(MessageEncoding::read_from(read)?, MessageEncoding::read_from(read)?),
            5 => Self::LeaderPath(read_vec(read)?),
            6 => Self::Peers(read_vec(read)?),
            7 => Self::Ok,
            8 => Self::ActionStreamClosed,
            9 => Self::RecoveryFailed(MessageEncoding::read_from(read)?),
            10 => Self::NoPathToLeader,
            11 => Self::LeaderInfo(MessageEncoding::read_from(read)?),
            12 => Self::Election(MessageEncoding::read_from(read)?),
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

fn write_opt_vec<T: MessageEncoding, W: std::io::Write>(v: &Option<Vec<T>>, out: &mut W) -> std::io::Result<usize> {
    let mut sum = v.is_some().write_to(out)?;
    if let Some(v) = v {
        sum += write_vec(v, out)?;
    }
    Ok(sum)
}

fn read_opt_vec<T: MessageEncoding, R: std::io::Read>(read: &mut R) -> std::io::Result<Option<Vec<T>>> {
    if bool::read_from(read)? {
        Ok(Some(read_vec(read)?))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct TestState;

    impl DeterministicState for TestState {
        type Action = u64;
        type AuthorityAction = u64;

        fn accept_seq(&self) -> u64 {
            0
        }

        fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
            action
        }

        fn update(&mut self, _action: &Self::AuthorityAction) {}
    }

    #[test]
    fn sync_request_tags_are_sequential() {
        let cases: Vec<(SyncRequest<u64, TestState>, u16)> = vec![
            (SyncRequest::ProtocolVersion(2), 0),
            (SyncRequest::MyAddress(1), 1),
            (SyncRequest::Ping(2), 2),
            (SyncRequest::SharePeers(vec![SharePeerDetails::from(3)]), 3),
            (SyncRequest::WhoIsLeader, 4),
            (SyncRequest::ShareLeaderPath, 5),
            (SyncRequest::ShareElection, 6),
            (SyncRequest::SubscribeFresh, 7),
            (SyncRequest::SubscribeRecovery(RecoverableStateDetails::new(4, 5)), 8),
            (SyncRequest::Action { source: 6, action: 7 }, 9),
            (
                SyncRequest::LeaderStatus {
                    address: 8,
                    status: LeaderStatus::Offline { term: 9, leader: 10 },
                },
                10,
            ),
        ];

        for (request, expected_tag) in cases {
            let mut bytes = Vec::new();
            request.write_to(&mut bytes).unwrap();

            assert_eq!(u16::read_from(&mut &bytes[..]).unwrap(), expected_tag);
        }
    }
}
