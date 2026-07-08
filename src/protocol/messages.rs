use std::{fmt::{Debug, Display}, num::NonZeroU64};

use message_encoding::MessageEncoding;

use crate::{
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails},
    },
    utils::{unknown_id_err},
};

pub const PROTOCOL_VERSION: u64 = 1;

pub enum SyncRequest<D: DeterministicState> {
    ProtocolVersion(u64),
    Subscribe(RecoverableStateDetails),
    Action { state_id: u64, action: RecoverableStateAction<D::Action> },
}


impl<D: DeterministicState> Debug for SyncRequest<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProtocolVersion(v) => write!(f, "ProtocolVersion({v})"),
            Self::Subscribe(details) => write!(f, "SubscribeRecovery({details:?})"),
            Self::Action { state_id, .. } => write!(f, "Action(state_id: {state_id})"),
        }
    }
}

pub enum SyncResponse<D: DeterministicState> {
    Ok,
    NotConnected,
    FreshState(RecoverableState<D>),
}

impl<D: DeterministicState> SyncResponse<D> {
    pub fn name(&self) -> &'static str {
        match self {
            SyncResponse::Ok => "Ok",
            SyncResponse::NotConnected => "NotConnected",
            SyncResponse::FreshState(_) => "FreshState",
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
        };

        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(match u16::read_from(read)? {
            0 => Self::Ok,
            1 => Self::NotConnected,
            2 => Self::FreshState(MessageEncoding::read_from(read)?),
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

// #[cfg(test)]
// mod tests {
//     use std::io::Result;
// 
//     use super::*;
// 
//     #[derive(Clone, Debug, PartialEq, Eq)]
//     struct TestState(u64);
// 
//     #[derive(Clone, Debug, PartialEq, Eq)]
//     struct TestAction(u64);
// 
//     impl DeterministicState for TestState {
//         type Action = TestAction;
//         type AuthorityAction = TestAction;
// 
//         fn accept_seq(&self) -> u64 {
//             self.0
//         }
// 
//         fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
//             action
//         }
// 
//         fn update(&mut self, _action: &Self::AuthorityAction) {
//             self.0 += 1;
//         }
//     }
// 
//     impl MessageEncoding for TestState {
//         fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
//             self.0.write_to(out)
//         }
// 
//         fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
//             Ok(Self(MessageEncoding::read_from(read)?))
//         }
//     }
// 
//     impl MessageEncoding for TestAction {
//         fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
//             self.0.write_to(out)
//         }
// 
//         fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
//             Ok(Self(MessageEncoding::read_from(read)?))
//         }
//     }
// 
//     fn leader_info() -> LeaderInfo<u64> {
//         LeaderInfo {
//             leader_state: LeaderState {
//                 term: ElectionTerm(2),
//                 mode: LeaderMode::Following { leader: 3 },
//             },
//             can_lead: true,
//             reachable_voters: vec![],
//             recovery_details: RecoverableStateDetails::new(1, 2),
//         }
//     }
// 
//     fn first_tag<M: MessageEncoding>(message: &M) -> u16 {
//         let mut bytes = Vec::new();
//         message.write_to(&mut bytes).unwrap();
//         u16::read_from(&mut &bytes[..]).unwrap()
//     }
// 
//     #[test]
//     fn leader_info_encoding_roundtrips() {
//         let info = leader_info();
//         let mut bytes = Vec::new();
// 
//         info.write_to(&mut bytes).unwrap();
// 
//         assert_eq!(u16::read_from(&mut &bytes[..]).unwrap(), 5);
// 
//         let decoded: LeaderInfo<u64> = LeaderInfo::read_from(&mut &bytes[..]).unwrap();
//         assert_eq!(decoded.leader_state, info.leader_state);
//         assert_eq!(decoded.can_lead, info.can_lead);
//     }
// 
//     #[test]
//     fn sync_request_tags_are_canonical() {
//         let cases: Vec<(SyncRequest<u64, TestState>, u16)> = vec![
//             (SyncRequest::ProtocolVersion(PROTOCOL_VERSION), 0),
//             (SyncRequest::MyAddress(1), 1),
//             (SyncRequest::SharePeers(vec![SharePeerDetails::from(3)]), 2),
//             (SyncRequest::LeaderInformation(leader_info()), 3),
//             (SyncRequest::SubscribeFresh, 4),
//             (SyncRequest::Subscribe(RecoverableStateDetails::new(4, 5)), 5),
//             (
//                 SyncRequest::Action {
//                     source: 6,
//                     action: TestAction(7),
//                 },
//                 6,
//             ),
//             (SyncRequest::LeaderQuery, 7),
//         ];
// 
//         for (request, tag) in cases {
//             assert_eq!(first_tag(&request), tag);
//         }
//     }
// 
//     #[test]
//     fn sync_response_tags_are_canonical() {
//         let cases: Vec<(SyncResponse<u64, TestState>, u16)> = vec![
//             (SyncResponse::Ok, 0),
//             (SyncResponse::FailedToQueueAction { source: 2 }, 1),
//             (SyncResponse::Peers(vec![SharePeerDetails::from(3)]), 2),
//             (SyncResponse::Accepted(6), 3),
//             (SyncResponse::RecoveryFailed, 4),
//             (SyncResponse::FreshState(RecoverableState::new(7, TestState(8))), 5),
//             (SyncResponse::AuthorityAction(9, RecoverableStateAction::StateAction { action: TestAction(10) }), 6),
//             (SyncResponse::ActionStreamClosed, 7),
//             (SyncResponse::UnexpectedRequest, 8),
//             (
//                 SyncResponse::LeaderState(LeaderState {
//                     term: ElectionTerm(3),
//                     mode: LeaderMode::Leading,
//                 }),
//                 9,
//             ),
//         ];
// 
//         for (response, tag) in cases {
//             assert_eq!(first_tag(&response), tag);
//         }
//     }
// }
