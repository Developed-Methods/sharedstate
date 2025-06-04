use std::{collections::VecDeque, fmt::Debug, hash::Hash, marker::PhantomData};
use message_encoding::{m_opt_sum, MessageEncoding};
use crate::{net::message_io::unknown_id_err, state::DeterministicState};

pub trait SourceId: Debug + Clone + Copy + Send + Sync + PartialEq + Eq + Hash + 'static {
}

impl<T: Debug + Clone + Copy + Send + Sync + PartialEq + Eq + Hash + 'static> SourceId for T {}

#[derive(Debug, PartialEq, Eq)]
pub struct RecoverableState<I: SourceId, D: DeterministicState> {
    details: RecoverableStateDetails,
    state: D,
    _phantom: PhantomData<I>,
}

impl<I: SourceId, D: DeterministicState + Clone> Clone for RecoverableState<I, D> {
    fn clone(&self) -> Self {
        RecoverableState {
            details: self.details.clone(),
            state: self.state.clone(),
            _phantom: PhantomData,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoverableStateDetails {
    pub sequence: u64,
    pub id: u64,
    pub generation: u64,
    pub state_sequence: u64,
    pub history: VecDeque<RecovGenerationEnd>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecovGenerationEnd {
    pub old_id: u64,
    pub generation: u64,
    pub next_sequence: u64,
}

impl RecoverableStateDetails {
    pub fn can_recover_follower(&self, follower: &RecoverableStateDetails) -> bool {
        /* follower is in the future */
        if self.generation < follower.generation {
            return false;
        }

        /* if in same generation, follow cannot be ahead of leader */
        if self.generation == follower.generation {
            return self.id == follower.id && follower.state_sequence <= self.state_sequence;
        }

        let depth = (self.generation - follower.generation) as usize;
        if self.history.len() < depth {
            /* don't have the history to recover */
            return false;
        }

        let hist = self.history.get(self.history.len() - depth).unwrap();
        assert_eq!(hist.generation, follower.generation);

        /* follower should not have advanced past upgrade in historic generation */
        hist.old_id == follower.id && follower.sequence <= hist.next_sequence
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoverableStateAction<I, A> {
    StateAction { source: I, action: A },
    BumpGeneration { new_id: u64 },
}

impl<I: SourceId + MessageEncoding, A: MessageEncoding> MessageEncoding for RecoverableStateAction<I, A> {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        match self {
            Self::StateAction { source, action } => Ok(1u16.write_to(out)? + source.write_to(out)? + action.write_to(out)?),
            Self::BumpGeneration { new_id } => Ok(2u16.write_to(out)? + new_id.write_to(out)?),
        }
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        match u16::read_from(read)? {
            1 => Ok(RecoverableStateAction::StateAction {
                source: MessageEncoding::read_from(read)?,
                action: MessageEncoding::read_from(read)?,
            }),
            2 => Ok(RecoverableStateAction::BumpGeneration { new_id: MessageEncoding::read_from(read)? }),
            other => Err(unknown_id_err(other, "RecoverableStateAction")),
        }
    }
}

impl<I: SourceId, D: DeterministicState> DeterministicState for RecoverableState<I, D> {
    type Action = RecoverableStateAction<I, D::Action>;
    type AuthorityAction = RecoverableStateAction<I, D::AuthorityAction>;

    fn sequence(&self) -> u64 {
        self.details.sequence
    }

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
        match action {
            RecoverableStateAction::StateAction { source, action } => RecoverableStateAction::StateAction { source, action: self.state.authority(action) },
            RecoverableStateAction::BumpGeneration { new_id } => RecoverableStateAction::BumpGeneration { new_id },
        }
    }

    fn update(&mut self, action: &Self::AuthorityAction) {
        self.details.sequence += 1;

        match action {
            RecoverableStateAction::StateAction { source: _, action } => {
                let seq = self.state.sequence();
                assert_eq!(self.details.state_sequence, seq);

                self.state.update(action);

                assert_eq!(seq + 1, self.state.sequence());
                self.details.state_sequence = seq + 1;
            }
            RecoverableStateAction::BumpGeneration { new_id } => {
                if 2048 <= self.details.history.len() {
                    let _ = self.details.history.pop_front();
                }

                self.details.history.push_back(RecovGenerationEnd {
                    old_id: self.details.id,
                    generation: self.details.generation,
                    next_sequence: self.details.sequence,
                });

                self.details.generation += 1;
                self.details.id = *new_id;
            }
        }
    }
}

impl<I: SourceId, D: DeterministicState> RecoverableState<I, D> {
    pub fn new(id: u64, state: D) -> Self {
        RecoverableState {
            details: RecoverableStateDetails {
                sequence: 1,
                id,
                generation: 1,
                state_sequence: state.sequence(),
                history: VecDeque::new(),
            },
            state,
            _phantom: PhantomData,
        }
    }

    pub fn details(&self) -> &RecoverableStateDetails {
        &self.details
    }

    pub fn state(&self) -> &D {
        &self.state
    }
}

impl MessageEncoding for RecovGenerationEnd {
    const STATIC_SIZE: Option<usize> = m_opt_sum(&[
        u64::STATIC_SIZE,
        u64::STATIC_SIZE,
    ]);

    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += self.old_id.write_to(out)?;
        sum += self.generation.write_to(out)?;
        sum += self.next_sequence.write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(RecovGenerationEnd {
            old_id: MessageEncoding::read_from(read)?,
            generation: MessageEncoding::read_from(read)?,
            next_sequence: MessageEncoding::read_from(read)?,
        })
    }
}

impl MessageEncoding for RecoverableStateDetails {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += self.sequence.write_to(out)?;
        sum += self.id.write_to(out)?;
        sum += self.generation.write_to(out)?;
        sum += self.state_sequence.write_to(out)?;
        sum += (self.history.len() as u64).write_to(out)?;
        for item in &self.history {
            sum += item.write_to(out)?;
        }
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(RecoverableStateDetails {
            sequence: MessageEncoding::read_from(read)?,
            id: MessageEncoding::read_from(read)?,
            generation: MessageEncoding::read_from(read)?,
            state_sequence: MessageEncoding::read_from(read)?,
            history: {
                let len = u64::read_from(read)? as usize;
                let mut history = VecDeque::with_capacity(len);

                for _ in 0..len {
                    history.push_back(RecovGenerationEnd::read_from(read)?);
                }

                history
            }
        })
    }
}

impl<I: SourceId, D: MessageEncoding + DeterministicState> MessageEncoding for RecoverableState<I, D> {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += self.details.write_to(out)?;
        sum += self.state.write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(RecoverableState {
            details: MessageEncoding::read_from(read)?,
            state: MessageEncoding::read_from(read)?,
            _phantom: PhantomData,
        })
    }
}

#[cfg(test)]
mod test {
    use message_encoding::{test_assert_valid_encoding, MessageEncoding};

    use crate::state::DeterministicState;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct MockState(u64);

    impl DeterministicState for MockState {
        type Action = ();
        type AuthorityAction = ();

        fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
            action
        }

        fn sequence(&self) -> u64 {
            self.0
        }

        fn update(&mut self, _: &Self::Action) {
            self.0 += 1;
        }
    }

    impl MessageEncoding for MockState {
        const STATIC_SIZE: Option<usize> = u64::STATIC_SIZE;
        const MAX_SIZE: Option<usize> = u64::MAX_SIZE;

        fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
            self.0.write_to(out)
        }
        fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
            Ok(MockState(MessageEncoding::read_from(read)?))
        }
    }

    #[test]
    fn state_recovery_check_test() {
        let mut state1 = RecoverableState::<u64, _>::new(101, MockState(12));
        let mut state2 = state1.clone();

        state1.update(&RecoverableStateAction::StateAction{ source: 3, action: () });
        state2.update(&RecoverableStateAction::StateAction{ source: 3, action: () });
        state1.update(&RecoverableStateAction::StateAction{ source: 3, action: () });
        state2.update(&RecoverableStateAction::StateAction{ source: 3, action: () });

        state1.update(&RecoverableStateAction::BumpGeneration { new_id: 1234 });
        state1.update(&RecoverableStateAction::StateAction{ source: 3, action: () });
        state1.update(&RecoverableStateAction::StateAction{ source: 3, action: () });

        assert!(state1.details.can_recover_follower(&state2.details));

        state2.update(&RecoverableStateAction::BumpGeneration { new_id: 1234 });
        state2.update(&RecoverableStateAction::StateAction{ source: 3, action: () });
        state2.update(&RecoverableStateAction::StateAction{ source: 3, action: () });

        assert_eq!(state1, state2);
    }

    #[test]
    fn recoverable_state_encoding_test() {
        test_assert_valid_encoding(RecoverableState {
            details: RecoverableStateDetails {
                id: 32145342,
                generation: 2,
                sequence: 3213,
                state_sequence: 321,
                history: vec![
                    RecovGenerationEnd {
                        old_id: 1241,
                        generation: 8,
                        next_sequence: 200,
                    },
                    RecovGenerationEnd {
                        old_id: 654,
                        generation: 9,
                        next_sequence: 280,
                    },
                ].into(),
            },
            state: MockState(300),
            _phantom: PhantomData::<u64>,
        });
    }
}
