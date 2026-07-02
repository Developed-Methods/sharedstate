use std::{fmt::Debug, num::NonZeroU64};

use message_encoding::MessageEncoding;

use crate::{
    state::{
        determinstic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails},
    },
    transport::traits::SyncIOAddress,
    utils::{unknown_id_err, unknown_version_err},
};

pub const PROTOCOL_VERSION: u64 = 1;

pub enum SyncRequest<A: SyncIOAddress, D: DeterministicState> {
    ProtocolVersion(u64),
    MyAddress(A),
    Ping(u64),
    SharePeers(Vec<SharePeerDetails<A>>),
    ShareLeaderPath,
    ShareLeaderInfo,
    SubscribeFresh,
    SubscribeRecovery(RecoverableStateDetails),
    Action { source: A, action: D::Action },
    LeaderInformation { source: A, info: LeaderWithElectionInfo<A> },
}

#[derive(Clone, Debug)]
pub enum LeaderStatus<A: SyncIOAddress> {
    Promoted { term: u64, leader: A },
    Offline { term: u64, leader: A },
    Observation(LeaderWithElectionInfo<A>),
}

#[derive(Clone, Debug)]
pub struct LeaderInfoMessage<A: SyncIOAddress> {
    pub leader: Option<A>,
    pub path: Option<Vec<A>>,
    pub term: u64,
}

#[derive(Clone, Debug)]
pub struct LeaderWithElectionInfo<A: SyncIOAddress> {
    pub observer: A,
    pub term: u64,
    pub leader: Option<A>,
    pub leader_path: Option<Vec<A>>,
    pub vote: Option<A>,
    pub can_lead: bool,
    pub reachable_can_lead: Vec<A>,
    pub recover_details: RecoverableStateDetails,
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
            Self::ShareLeaderPath => write!(f, "ShareLeaderPath"),
            Self::ShareLeaderInfo => write!(f, "ShareLeaderInfo"),
            Self::SubscribeFresh => write!(f, "SubscribeFresh"),
            Self::SubscribeRecovery(details) => write!(f, "SubscribeRecovery({details:?})"),
            Self::Action { source, .. } => write!(f, "Action(source: {source:?})"),
            Self::LeaderInformation { source, info } => {
                write!(f, "LeaderInformation(source: {source:?}, info: {info:?})")
            }
        }
    }
}

pub enum SyncResponse<A: SyncIOAddress, D: DeterministicState> {
    Pong(u64),
    Ok,
    FailedToQueueAction { source: A },
    Peers(Vec<SharePeerDetails<A>>),
    LeaderInfo(LeaderWithElectionInfo<A>),
    LeaderPath(Vec<A>),
    NoPathToLeader,
    Accepted(u64),
    RecoveryFailed,
    FreshState(RecoverableState<D>),
    AuthorityAction(u64, RecoverableStateAction<D::AuthorityAction>),
    ActionStreamClosed,
    UnexpectedRequest,
}

impl<A: SyncIOAddress, D: DeterministicState> SyncResponse<A, D> {
    pub fn name(&self) -> &'static str {
        match self {
            SyncResponse::Pong(_) => "Pong",
            SyncResponse::Ok => "Ok",
            SyncResponse::FailedToQueueAction { .. } => "FailedToQueueAction",
            SyncResponse::Peers(_) => "Peers",
            SyncResponse::LeaderInfo(_) => "LeaderInfo",
            SyncResponse::LeaderPath(_) => "LeaderPath",
            SyncResponse::NoPathToLeader => "NoPathToLeader",
            SyncResponse::Accepted(_) => "Accepted",
            SyncResponse::RecoveryFailed => "RecoveryFailed",
            SyncResponse::FreshState(_) => "FreshState",
            SyncResponse::AuthorityAction(_, _) => "AuthorityAction",
            SyncResponse::ActionStreamClosed => "ActionStreamClosed",
            SyncResponse::UnexpectedRequest => "UnexpectedRequest",
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
            Self::ShareLeaderPath => 4u16.write_to(out)?,
            Self::ShareLeaderInfo => 5u16.write_to(out)?,
            Self::SubscribeFresh => 6u16.write_to(out)?,
            Self::SubscribeRecovery(details) => {
                sum += 7u16.write_to(out)?;
                details.write_to(out)?
            }
            Self::Action { source, action } => {
                sum += 8u16.write_to(out)?;
                sum += source.write_to(out)?;
                action.write_to(out)?
            }
            Self::LeaderInformation { source, info } => {
                sum += 9u16.write_to(out)?;
                sum += source.write_to(out)?;
                info.write_to(out)?
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
            4 => Self::ShareLeaderPath,
            5 => Self::ShareLeaderInfo,
            6 => Self::SubscribeFresh,
            7 => Self::SubscribeRecovery(MessageEncoding::read_from(read)?),
            8 => Self::Action {
                source: MessageEncoding::read_from(read)?,
                action: MessageEncoding::read_from(read)?,
            },
            9 => Self::LeaderInformation {
                source: MessageEncoding::read_from(read)?,
                info: MessageEncoding::read_from(read)?,
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

impl<A: SyncIOAddress> MessageEncoding for LeaderWithElectionInfo<A> {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += 3u16.write_to(out)?;
        sum += self.observer.write_to(out)?;
        sum += self.term.write_to(out)?;
        sum += self.leader.write_to(out)?;
        sum += write_opt_vec(&self.leader_path, out)?;
        sum += self.vote.write_to(out)?;
        sum += self.can_lead.write_to(out)?;
        sum += write_vec(&self.reachable_can_lead, out)?;
        sum += self.recover_details.write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        let version = u16::read_from(read)?;
        if version != 3 {
            return Err(unknown_version_err(version, "ElectionObservation"));
        }

        Ok(Self {
            observer: MessageEncoding::read_from(read)?,
            term: MessageEncoding::read_from(read)?,
            leader: MessageEncoding::read_from(read)?,
            leader_path: read_opt_vec(read)?,
            vote: MessageEncoding::read_from(read)?,
            can_lead: MessageEncoding::read_from(read)?,
            reachable_can_lead: read_vec(read)?,
            recover_details: MessageEncoding::read_from(read)?,
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
                sum += 0u16.write_to(out)?;
                num.write_to(out)?
            }
            Self::Ok => 1u16.write_to(out)?,
            Self::FailedToQueueAction { source } => {
                sum += 2u16.write_to(out)?;
                source.write_to(out)?
            }
            Self::Peers(peers) => {
                sum += 3u16.write_to(out)?;
                write_vec(peers, out)?
            }
            Self::LeaderInfo(info) => {
                sum += 4u16.write_to(out)?;
                info.write_to(out)?
            }
            Self::LeaderPath(path) => {
                sum += 5u16.write_to(out)?;
                write_vec(path, out)?
            }
            Self::NoPathToLeader => 6u16.write_to(out)?,
            Self::Accepted(next_seq) => {
                sum += 7u16.write_to(out)?;
                next_seq.write_to(out)?
            }
            Self::RecoveryFailed => 8u16.write_to(out)?,
            Self::FreshState(state) => {
                sum += 9u16.write_to(out)?;
                state.write_to(out)?
            }
            Self::AuthorityAction(seq, action) => {
                sum += 10u16.write_to(out)?;
                sum += seq.write_to(out)?;
                action.write_to(out)?
            }
            Self::ActionStreamClosed => 11u16.write_to(out)?,
            Self::UnexpectedRequest => 12u16.write_to(out)?,
        };

        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(match u16::read_from(read)? {
            0 => Self::Pong(MessageEncoding::read_from(read)?),
            1 => Self::Ok,
            2 => Self::FailedToQueueAction {
                source: MessageEncoding::read_from(read)?,
            },
            3 => Self::Peers(read_vec(read)?),
            4 => Self::LeaderInfo(MessageEncoding::read_from(read)?),
            5 => Self::LeaderPath(read_vec(read)?),
            6 => Self::NoPathToLeader,
            7 => Self::Accepted(MessageEncoding::read_from(read)?),
            8 => Self::RecoveryFailed,
            9 => Self::FreshState(MessageEncoding::read_from(read)?),
            10 => Self::AuthorityAction(MessageEncoding::read_from(read)?, MessageEncoding::read_from(read)?),
            11 => Self::ActionStreamClosed,
            12 => Self::UnexpectedRequest,
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
    use std::io::Result;

    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestState(u64);

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestAction(u64);

    impl DeterministicState for TestState {
        type Action = TestAction;
        type AuthorityAction = TestAction;

        fn accept_seq(&self) -> u64 {
            self.0
        }

        fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
            action
        }

        fn update(&mut self, _action: &Self::AuthorityAction) {
            self.0 += 1;
        }
    }

    impl MessageEncoding for TestState {
        fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
            self.0.write_to(out)
        }

        fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
            Ok(Self(MessageEncoding::read_from(read)?))
        }
    }

    impl MessageEncoding for TestAction {
        fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
            self.0.write_to(out)
        }

        fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
            Ok(Self(MessageEncoding::read_from(read)?))
        }
    }

    fn leader_info() -> LeaderWithElectionInfo<u64> {
        LeaderWithElectionInfo {
            observer: 1,
            term: 2,
            leader: Some(3),
            leader_path: Some(vec![3, 1]),
            vote: Some(3),
            can_lead: true,
            reachable_can_lead: vec![1, 3],
            recover_details: RecoverableStateDetails::new(1, 1),
        }
    }

    fn first_tag<M: MessageEncoding>(message: &M) -> u16 {
        let mut bytes = Vec::new();
        message.write_to(&mut bytes).unwrap();
        u16::read_from(&mut &bytes[..]).unwrap()
    }

    #[test]
    fn leader_info_encoding_version_three_roundtrips_vote() {
        let info = leader_info();
        let mut bytes = Vec::new();

        info.write_to(&mut bytes).unwrap();

        assert_eq!(u16::read_from(&mut &bytes[..]).unwrap(), 3);

        let decoded: LeaderWithElectionInfo<u64> = LeaderWithElectionInfo::read_from(&mut &bytes[..]).unwrap();
        assert_eq!(decoded.observer, info.observer);
        assert_eq!(decoded.term, info.term);
        assert_eq!(decoded.leader, info.leader);
        assert_eq!(decoded.leader_path, info.leader_path);
        assert_eq!(decoded.vote, info.vote);
        assert_eq!(decoded.can_lead, info.can_lead);
        assert_eq!(decoded.reachable_can_lead, info.reachable_can_lead);
        assert_eq!(decoded.recover_details.next_seq(), info.recover_details.next_seq());
    }

    #[test]
    fn sync_request_tags_are_canonical() {
        let cases: Vec<(SyncRequest<u64, TestState>, u16)> = vec![
            (SyncRequest::ProtocolVersion(PROTOCOL_VERSION), 0),
            (SyncRequest::MyAddress(1), 1),
            (SyncRequest::Ping(2), 2),
            (SyncRequest::SharePeers(vec![SharePeerDetails::from(3)]), 3),
            (SyncRequest::ShareLeaderPath, 4),
            (SyncRequest::ShareLeaderInfo, 5),
            (SyncRequest::SubscribeFresh, 6),
            (SyncRequest::SubscribeRecovery(RecoverableStateDetails::new(4, 5)), 7),
            (
                SyncRequest::Action {
                    source: 6,
                    action: TestAction(7),
                },
                8,
            ),
            (
                SyncRequest::LeaderInformation {
                    source: 8,
                    info: leader_info(),
                },
                9,
            ),
        ];

        for (request, tag) in cases {
            assert_eq!(first_tag(&request), tag);
        }
    }

    #[test]
    fn sync_response_tags_are_canonical() {
        let cases: Vec<(SyncResponse<u64, TestState>, u16)> = vec![
            (SyncResponse::Pong(1), 0),
            (SyncResponse::Ok, 1),
            (SyncResponse::FailedToQueueAction { source: 2 }, 2),
            (SyncResponse::Peers(vec![SharePeerDetails::from(3)]), 3),
            (SyncResponse::LeaderInfo(leader_info()), 4),
            (SyncResponse::LeaderPath(vec![4, 5]), 5),
            (SyncResponse::NoPathToLeader, 6),
            (SyncResponse::Accepted(6), 7),
            (SyncResponse::RecoveryFailed, 8),
            (SyncResponse::FreshState(RecoverableState::new(7, TestState(8))), 9),
            (SyncResponse::AuthorityAction(9, RecoverableStateAction::StateAction { action: TestAction(10) }), 10),
            (SyncResponse::ActionStreamClosed, 11),
            (SyncResponse::UnexpectedRequest, 12),
        ];

        for (response, tag) in cases {
            assert_eq!(first_tag(&response), tag);
        }
    }
}
