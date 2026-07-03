use std::{fmt::Debug, num::NonZeroU64};

use message_encoding::MessageEncoding;

use crate::{
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails},
    },
    transport::traits::SyncIOAddress,
    utils::{unknown_id_err, unknown_version_err},
};

pub const PROTOCOL_VERSION: u64 = 1;

pub enum SyncRequest<A: SyncIOAddress, D: DeterministicState> {
    ProtocolVersion(u64),
    MyAddress(A),
    SharePeers(Vec<SharePeerDetails<A>>),
    LeaderInformation(LeaderInfo<A>),

    SubscribeFresh,
    SubscribeRecovery(RecoverableStateDetails),
    Action { source: A, action: D::Action },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderState<A: SyncIOAddress> {
    pub term: u64,
    pub mode: LeaderMode<A>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LeaderMode<A: SyncIOAddress> {
    NoLeader,
    Electing { vote: Option<A> },
    Leading,
    Following { leader: A },
}

#[derive(Clone, Debug)]
pub struct LeaderInfo<A: SyncIOAddress> {
    pub leader_state: LeaderState<A>,
    pub can_lead: bool,
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
            Self::SharePeers(peers) => write!(f, "SharePeers({peers:?})"),
            Self::LeaderInformation(info) => write!(f, "LeaderInformation({info:?})"),
            Self::SubscribeFresh => write!(f, "SubscribeFresh"),
            Self::SubscribeRecovery(details) => write!(f, "SubscribeRecovery({details:?})"),
            Self::Action { source, .. } => write!(f, "Action(source: {source:?})"),
        }
    }
}

pub enum SyncResponse<A: SyncIOAddress, D: DeterministicState> {
    Ok,
    FailedToQueueAction { source: A },
    Peers(Vec<SharePeerDetails<A>>),
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
            SyncResponse::Ok => "Ok",
            SyncResponse::FailedToQueueAction { .. } => "FailedToQueueAction",
            SyncResponse::Peers(_) => "Peers",
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
            Self::SharePeers(peers) => {
                sum += 2u16.write_to(out)?;
                write_vec(peers, out)?
            }
            Self::LeaderInformation(info) => {
                sum += 3u16.write_to(out)?;
                info.write_to(out)?
            }
            Self::SubscribeFresh => 4u16.write_to(out)?,
            Self::SubscribeRecovery(details) => {
                sum += 5u16.write_to(out)?;
                details.write_to(out)?
            }
            Self::Action { source, action } => {
                sum += 6u16.write_to(out)?;
                sum += source.write_to(out)?;
                action.write_to(out)?
            }
        };

        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(match u16::read_from(read)? {
            0 => Self::ProtocolVersion(MessageEncoding::read_from(read)?),
            1 => Self::MyAddress(MessageEncoding::read_from(read)?),
            2 => Self::SharePeers(read_vec(read)?),
            3 => Self::LeaderInformation(MessageEncoding::read_from(read)?),
            4 => Self::SubscribeFresh,
            5 => Self::SubscribeRecovery(MessageEncoding::read_from(read)?),
            6 => Self::Action {
                source: MessageEncoding::read_from(read)?,
                action: MessageEncoding::read_from(read)?,
            },
            other => return Err(unknown_id_err(other, "SyncRequest")),
        })
    }
}

impl<A: SyncIOAddress> MessageEncoding for LeaderInfo<A> {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += 5u16.write_to(out)?;
        sum += self.leader_state.write_to(out)?;
        sum += self.can_lead.write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        let version = u16::read_from(read)?;
        if version != 5 {
            return Err(unknown_version_err(version, "LeaderInfo"));
        }

        Ok(Self {
            leader_state: MessageEncoding::read_from(read)?,
            can_lead: MessageEncoding::read_from(read)?,
        })
    }
}

impl<A: SyncIOAddress> MessageEncoding for LeaderState<A> {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += 1u16.write_to(out)?;
        sum += self.term.write_to(out)?;
        sum += self.mode.write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        let version = u16::read_from(read)?;
        if version != 1 {
            return Err(unknown_version_err(version, "LeaderState"));
        }

        Ok(Self {
            term: MessageEncoding::read_from(read)?,
            mode: MessageEncoding::read_from(read)?,
        })
    }
}

impl<A: SyncIOAddress> MessageEncoding for LeaderMode<A> {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        match self {
            LeaderMode::NoLeader => {
                sum += 0u16.write_to(out)?;
            }
            LeaderMode::Electing { vote } => {
                sum += 1u16.write_to(out)?;
                sum += vote.write_to(out)?;
            }
            LeaderMode::Leading => {
                sum += 2u16.write_to(out)?;
            }
            LeaderMode::Following { leader } => {
                sum += 3u16.write_to(out)?;
                sum += leader.write_to(out)?;
            }
        }
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        let tag = u16::read_from(read)?;
        match tag {
            0 => Ok(LeaderMode::NoLeader),
            1 => Ok(LeaderMode::Electing {
                vote: MessageEncoding::read_from(read)?,
            }),
            2 => Ok(LeaderMode::Leading),
            3 => Ok(LeaderMode::Following {
                leader: MessageEncoding::read_from(read)?,
            }),
            _ => Err(unknown_id_err(tag, "LeaderMode")),
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
            Self::Ok => 0u16.write_to(out)?,
            Self::FailedToQueueAction { source } => {
                sum += 1u16.write_to(out)?;
                source.write_to(out)?
            }
            Self::Peers(peers) => {
                sum += 2u16.write_to(out)?;
                write_vec(peers, out)?
            }
            Self::Accepted(next_seq) => {
                sum += 3u16.write_to(out)?;
                next_seq.write_to(out)?
            }
            Self::RecoveryFailed => 4u16.write_to(out)?,
            Self::FreshState(state) => {
                sum += 5u16.write_to(out)?;
                state.write_to(out)?
            }
            Self::AuthorityAction(seq, action) => {
                sum += 6u16.write_to(out)?;
                sum += seq.write_to(out)?;
                action.write_to(out)?
            }
            Self::ActionStreamClosed => 7u16.write_to(out)?,
            Self::UnexpectedRequest => 8u16.write_to(out)?,
        };

        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(match u16::read_from(read)? {
            0 => Self::Ok,
            1 => Self::FailedToQueueAction {
                source: MessageEncoding::read_from(read)?,
            },
            2 => Self::Peers(read_vec(read)?),
            3 => Self::Accepted(MessageEncoding::read_from(read)?),
            4 => Self::RecoveryFailed,
            5 => Self::FreshState(MessageEncoding::read_from(read)?),
            6 => Self::AuthorityAction(MessageEncoding::read_from(read)?, MessageEncoding::read_from(read)?),
            7 => Self::ActionStreamClosed,
            8 => Self::UnexpectedRequest,
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

    fn leader_info() -> LeaderInfo<u64> {
        LeaderInfo {
            leader_state: LeaderState {
                term: 2,
                mode: LeaderMode::Following { leader: 3 },
            },
            can_lead: true,
        }
    }

    fn first_tag<M: MessageEncoding>(message: &M) -> u16 {
        let mut bytes = Vec::new();
        message.write_to(&mut bytes).unwrap();
        u16::read_from(&mut &bytes[..]).unwrap()
    }

    #[test]
    fn leader_info_encoding_roundtrips() {
        let info = leader_info();
        let mut bytes = Vec::new();

        info.write_to(&mut bytes).unwrap();

        assert_eq!(u16::read_from(&mut &bytes[..]).unwrap(), 5);

        let decoded: LeaderInfo<u64> = LeaderInfo::read_from(&mut &bytes[..]).unwrap();
        assert_eq!(decoded.leader_state, info.leader_state);
        assert_eq!(decoded.can_lead, info.can_lead);
    }

    #[test]
    fn sync_request_tags_are_canonical() {
        let cases: Vec<(SyncRequest<u64, TestState>, u16)> = vec![
            (SyncRequest::ProtocolVersion(PROTOCOL_VERSION), 0),
            (SyncRequest::MyAddress(1), 1),
            (SyncRequest::SharePeers(vec![SharePeerDetails::from(3)]), 2),
            (SyncRequest::LeaderInformation(leader_info()), 3),
            (SyncRequest::SubscribeFresh, 4),
            (SyncRequest::SubscribeRecovery(RecoverableStateDetails::new(4, 5)), 5),
            (
                SyncRequest::Action {
                    source: 6,
                    action: TestAction(7),
                },
                6,
            ),
        ];

        for (request, tag) in cases {
            assert_eq!(first_tag(&request), tag);
        }
    }

    #[test]
    fn sync_response_tags_are_canonical() {
        let cases: Vec<(SyncResponse<u64, TestState>, u16)> = vec![
            (SyncResponse::Ok, 0),
            (SyncResponse::FailedToQueueAction { source: 2 }, 1),
            (SyncResponse::Peers(vec![SharePeerDetails::from(3)]), 2),
            (SyncResponse::Accepted(6), 3),
            (SyncResponse::RecoveryFailed, 4),
            (SyncResponse::FreshState(RecoverableState::new(7, TestState(8))), 5),
            (SyncResponse::AuthorityAction(9, RecoverableStateAction::StateAction { action: TestAction(10) }), 6),
            (SyncResponse::ActionStreamClosed, 7),
            (SyncResponse::UnexpectedRequest, 8),
        ];

        for (response, tag) in cases {
            assert_eq!(first_tag(&response), tag);
        }
    }
}
