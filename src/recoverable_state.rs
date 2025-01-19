use std::collections::VecDeque;
use message_encoding::{m_opt_sum, m_static, MessageEncoding};
use crate::{message_io::unknown_id_err, state::DeterministicState};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoverableState<D: DeterministicState> {
    generations_updated_till_seq: u64,
    details: RecoverableStateDetails,
    state: D,
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
    pub generation: u64,
    pub next_sequence: u64,
}

impl RecoverableStateDetails {
    pub fn can_recover_follower(&self, follower: &RecoverableStateDetails) -> bool {
        if self.id != follower.id {
            return false;
        }

        /* follower is in the future */
        if self.generation < follower.generation {
            return false;
        }

        if self.generation == follower.generation {
            return follower.state_sequence <= self.state_sequence;
        }

        let depth = (self.generation - follower.generation) as usize;
        if self.history.len() < depth {
            /* don't have the history to recover */
            return false;
        }

        let hist = self.history.get(self.history.len() - depth).unwrap();
        assert_eq!(hist.generation, follower.generation);

        /* follower should not have advanced past upgrade in historic generation */
        follower.sequence <= hist.next_sequence
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RecoverableStateAction<A> {
    StateAction(A),
    BumpGeneration,
}

impl<A: MessageEncoding> MessageEncoding for RecoverableStateAction<A> {
    const STATIC_SIZE: Option<usize> = m_opt_sum(&[
        u16::STATIC_SIZE,
        A::STATIC_SIZE,
    ]);

    const MAX_SIZE: Option<usize> = m_opt_sum(&[
        u16::MAX_SIZE,
        A::MAX_SIZE,
    ]);

    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        match self {
            Self::StateAction(a) => Ok(1u16.write_to(out)? + a.write_to(out)?),
            Self::BumpGeneration => 2u16.write_to(out),
        }
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        match u16::read_from(read)? {
            1 => Ok(RecoverableStateAction::StateAction(MessageEncoding::read_from(read)?)),
            2 => Ok(RecoverableStateAction::BumpGeneration),
            other => Err(unknown_id_err(other, "RecoverableStateAction")),
        }
    }
}

impl<D: DeterministicState> DeterministicState for RecoverableState<D> {
    type Action = RecoverableStateAction<D::Action>;
    type AuthorityAction = RecoverableStateAction<D::AuthorityAction>;

    fn sequence(&self) -> u64 {
        self.details.sequence
    }

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
        match action {
            RecoverableStateAction::StateAction(a) => RecoverableStateAction::StateAction(self.state.authority(a)),
            RecoverableStateAction::BumpGeneration => RecoverableStateAction::BumpGeneration,
        }
    }

    fn update(&mut self, action: &Self::AuthorityAction) {
        self.details.sequence += 1;
        if self.generations_updated_till_seq == self.details.sequence {
            self.generations_updated_till_seq = 0;
        }

        match action {
            RecoverableStateAction::StateAction(a) => {
                let seq = self.state.sequence();
                assert_eq!(self.details.state_sequence, seq);

                self.state.update(a);

                assert_eq!(seq + 1, self.state.sequence());
                self.details.state_sequence = seq + 1;
            }
            RecoverableStateAction::BumpGeneration => {
                if self.details.sequence < self.generations_updated_till_seq {
                    return;
                }

                if 2048 <= self.details.history.len() {
                    let _ = self.details.history.pop_back();
                }

                self.details.history.push_back(RecovGenerationEnd {
                    generation: self.details.generation,
                    next_sequence: self.details.sequence,
                });

                self.details.generation += 1;
            }
        }
    }
}

impl<D: DeterministicState> RecoverableState<D> {
    pub fn new(id: u64, state: D) -> Self {
        RecoverableState {
            generations_updated_till_seq: 0,
            details: RecoverableStateDetails {
                sequence: 1,
                id,
                generation: 1,
                state_sequence: state.sequence(),
                history: VecDeque::new(),
            },
            state,
        }
    }

    pub fn details(&self) -> &RecoverableStateDetails {
        &self.details
    }

    pub fn upgrade(&mut self, leader: &RecoverableStateDetails) -> bool {
        if !leader.can_recover_follower(&self.details) {
            return false;
        }

        self.generations_updated_till_seq = leader.sequence;
        self.details.generation = leader.generation;
        self.details.history = leader.history.clone();
        true
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
        sum += self.generation.write_to(out)?;
        sum += self.next_sequence.write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(RecovGenerationEnd {
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

impl<D: MessageEncoding + DeterministicState> MessageEncoding for RecoverableState<D> {
    const MAX_SIZE: Option<usize> = m_opt_sum(&[
        u64::MAX_SIZE,
        u64::MAX_SIZE,
        Some(m_static::<u64>() * 2 * 2048),
        D::MAX_SIZE,
    ]);

    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += self.generations_updated_till_seq.write_to(out)?;
        sum += self.details.write_to(out)?;
        sum += self.state.write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(RecoverableState {
            generations_updated_till_seq: MessageEncoding::read_from(read)?,
            details: MessageEncoding::read_from(read)?,
            state: MessageEncoding::read_from(read)?,
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
        let mut state1 = RecoverableState::new(101, MockState(12));
        let mut state2 = state1.clone();

        state1.update(&RecoverableStateAction::StateAction(()));
        state2.update(&RecoverableStateAction::StateAction(()));
        state1.update(&RecoverableStateAction::StateAction(()));
        state2.update(&RecoverableStateAction::StateAction(()));

        state1.update(&RecoverableStateAction::BumpGeneration);
        state1.update(&RecoverableStateAction::StateAction(()));
        state1.update(&RecoverableStateAction::StateAction(()));

        assert!(state1.details.can_recover_follower(&state2.details));
        state2.upgrade(&state1.details);

        state2.update(&RecoverableStateAction::BumpGeneration);
        state2.update(&RecoverableStateAction::StateAction(()));
        state2.update(&RecoverableStateAction::StateAction(()));

        assert_eq!(state1, state2);
    }

    #[test]
    fn recoverable_state_encoding_test() {
        test_assert_valid_encoding(RecoverableState {
            generations_updated_till_seq: 10,
            details: RecoverableStateDetails {
                id: 32145342,
                generation: 2,
                sequence: 3213,
                state_sequence: 321,
                history: vec![
                    RecovGenerationEnd {
                        generation: 8,
                        next_sequence: 200,
                    },
                    RecovGenerationEnd {
                        generation: 9,
                        next_sequence: 280,
                    },
                ].into(),
            },
            state: MockState(300),
        });
    }
}
