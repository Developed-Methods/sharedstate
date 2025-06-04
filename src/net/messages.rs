use message_encoding::MessageEncoding;

use crate::{recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails}, state::DeterministicState};

use super::{io::SyncIO, message_io::unknown_id_err};

pub enum SyncRequest<I: SyncIO, D: DeterministicState> {
    Ping(u64),
    SubscribeFresh,
    WhoisLeader,
    SendMePeers,
    NoticePeers(Vec<I::Address>),
    SubscribeRecovery(RecoverableStateDetails),
    Action { source: I::Address, action: D::Action },
}

pub enum SyncResponse<I: SyncIO, D: DeterministicState> {
    Pong(u64),
    RecoveryAccepted(u64),
    FreshState(RecoverableState<I::Address, D>),
    AuthorityAction(u64, RecoverableStateAction<I::Address, D::AuthorityAction>),
    Leader(I::Address),
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
            Self::WhoisLeader => 4u16.write_to(out)?,
            Self::SendMePeers => 5u16.write_to(out)?,
            Self::NoticePeers(peers) => {
                sum += 6u16.write_to(out)?;
                sum += (peers.len() as u64).write_to(out)?;
                for peer in peers {
                    sum += peer.write_to(out)?;
                }
                0
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
            4 => Self::WhoisLeader,
            5 => Self::SendMePeers,
            6 => Self::NoticePeers({
                let count = u64::read_from(read)? as usize;
                let mut peers = Vec::with_capacity(count);
                for _ in 0..count {
                    peers.push(MessageEncoding::read_from(read)?);
                }
                peers
            }),
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
            Self::Leader(addr) => {
                sum += 5u16.write_to(out)?;
                addr.write_to(out)?
            }
            Self::Peers(peers) => {
                sum += 6u16.write_to(out)?;
                sum += (peers.len() as u64).write_to(out)?;
                for peer in peers {
                    sum += peer.write_to(out)?;
                }
                0
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
            5 => Self::Leader(MessageEncoding::read_from(read)?),
            6 => Self::Peers({
                let count = u64::read_from(read)? as usize;
                let mut peers = Vec::with_capacity(count);
                for _ in 0..count {
                    peers.push(MessageEncoding::read_from(read)?);
                }
                peers
            }),
            other => return Err(unknown_id_err(other, "SyncResponse")),
        })
    }
}
