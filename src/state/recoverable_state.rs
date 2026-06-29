use super::determinstic_state::DeterministicState;
use crate::utils::unknown_id_err;
use message_encoding::{m_opt_sum, MessageEncoding};
use std::{collections::VecDeque, fmt::Debug};

#[derive(Debug, PartialEq, Eq)]
pub struct RecoverableState<D: DeterministicState> {
    details: RecoverableStateDetails,
    state: D,
}

impl<D: DeterministicState> RecoverableState<D> {
    pub fn new(id: u64, state: D) -> Self {
        RecoverableState {
            details: RecoverableStateDetails::new(id, state.accept_seq()),
            state,
        }
    }

    pub fn details(&self) -> &RecoverableStateDetails {
        &self.details
    }

    pub fn state(&self) -> &D {
        &self.state
    }
}

impl<D: DeterministicState + Clone> Clone for RecoverableState<D> {
    fn clone(&self) -> Self {
        RecoverableState {
            details: self.details.clone(),
            state: self.state.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoverableStateDetails {
    id: u64,
    generation: u64,
    outer_state_next_seq: u64,
    inner_state_next_seq: u64,
    history: VecDeque<RecovGenerationEnd>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecovGenerationEnd {
    old_id: u64,
    generation: u64,
    next_sequence: u64,
}

impl RecoverableStateDetails {
    pub fn new(id: u64, accept_sequence: u64) -> Self {
        RecoverableStateDetails {
            id,
            outer_state_next_seq: 1,
            generation: 1,
            inner_state_next_seq: accept_sequence,
            history: VecDeque::new(),
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.outer_state_next_seq
    }

    pub fn apply_inner_state_seq(&mut self, seq: u64) {
        assert_eq!(self.inner_state_next_seq, seq);
        self.inner_state_next_seq = seq + 1;
        self.outer_state_next_seq += 1;
    }

    pub fn apply_generation_bump(&mut self, new_id: u64) {
        self.outer_state_next_seq += 1;

        if 2048 <= self.history.len() {
            let _ = self.history.pop_front();
        }

        self.history.push_back(RecovGenerationEnd {
            old_id: self.id,
            generation: self.generation,
            next_sequence: self.outer_state_next_seq,
        });

        self.generation += 1;
        self.id = new_id;
    }

    pub fn progress_recover(&mut self, seq: u64) -> bool {
        while let Some(oldest) = self.history.front() {
            if seq < oldest.next_sequence {
                return false;
            }

            self.history.pop_front();
        }

        self.history.is_empty()
    }

    pub fn can_recover_follower(&self, follower: &RecoverableStateDetails) -> bool {
        /* follower is in the future */
        if self.generation < follower.generation {
            return false;
        }

        /* if in same generation, follow cannot be ahead of leader */
        if self.generation == follower.generation {
            return self.id == follower.id && follower.inner_state_next_seq <= self.inner_state_next_seq;
        }

        let depth = (self.generation - follower.generation) as usize;
        if self.history.len() < depth {
            /* don't have the history to recover */
            return false;
        }

        let hist = self.history.get(self.history.len() - depth).unwrap();
        assert_eq!(hist.generation, follower.generation);

        /* follower should not have advanced past upgrade in historic generation */
        hist.old_id == follower.id && follower.outer_state_next_seq < hist.next_sequence
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoverableStateAction<A> {
    StateAction { action: A },
    BumpGeneration { new_id: u64 },
}

impl<A: MessageEncoding> MessageEncoding for RecoverableStateAction<A> {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        match self {
            Self::StateAction { action } => Ok(3u16.write_to(out)? + action.write_to(out)?),
            Self::BumpGeneration { new_id } => Ok(2u16.write_to(out)? + new_id.write_to(out)?),
        }
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        match u16::read_from(read)? {
            3 => Ok(RecoverableStateAction::StateAction {
                action: MessageEncoding::read_from(read)?,
            }),
            2 => Ok(RecoverableStateAction::BumpGeneration {
                new_id: MessageEncoding::read_from(read)?,
            }),
            other => Err(unknown_id_err(other, "RecoverableStateAction")),
        }
    }
}

impl<D: DeterministicState> DeterministicState for RecoverableState<D> {
    type Action = RecoverableStateAction<D::Action>;
    type AuthorityAction = RecoverableStateAction<D::AuthorityAction>;

    fn accept_seq(&self) -> u64 {
        self.details.next_seq()
    }

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
        match action {
            RecoverableStateAction::StateAction { action } => RecoverableStateAction::StateAction {
                action: self.state.authority(action),
            },
            RecoverableStateAction::BumpGeneration { new_id } => RecoverableStateAction::BumpGeneration { new_id },
        }
    }

    fn update(&mut self, action: &Self::AuthorityAction) {
        match action {
            RecoverableStateAction::StateAction { action } => {
                let seq = self.state.accept_seq();
                self.details.apply_inner_state_seq(seq);

                self.state.update(action);
                assert_eq!(seq + 1, self.state.accept_seq());
            }
            RecoverableStateAction::BumpGeneration { new_id } => {
                self.details.apply_generation_bump(*new_id);
            }
        }
    }
}

impl MessageEncoding for RecovGenerationEnd {
    const STATIC_SIZE: Option<usize> = m_opt_sum(&[
        u64::STATIC_SIZE,
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
        sum += self.outer_state_next_seq.write_to(out)?;
        sum += self.id.write_to(out)?;
        sum += self.generation.write_to(out)?;
        sum += self.inner_state_next_seq.write_to(out)?;
        sum += (self.history.len() as u64).write_to(out)?;
        for item in &self.history {
            sum += item.write_to(out)?;
        }
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(RecoverableStateDetails {
            outer_state_next_seq: MessageEncoding::read_from(read)?,
            id: MessageEncoding::read_from(read)?,
            generation: MessageEncoding::read_from(read)?,
            inner_state_next_seq: MessageEncoding::read_from(read)?,
            history: {
                let len = u64::read_from(read)? as usize;
                let mut history = VecDeque::with_capacity(len);

                for _ in 0..len {
                    history.push_back(RecovGenerationEnd::read_from(read)?);
                }

                history
            },
        })
    }
}

impl<D: MessageEncoding + DeterministicState> MessageEncoding for RecoverableState<D> {
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
        })
    }
}

#[cfg(test)]
mod test {
    use message_encoding::{test_assert_valid_encoding, MessageEncoding};

    use crate::state::determinstic_state::DeterministicState;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct MockState(u64);

    impl DeterministicState for MockState {
        type Action = ();
        type AuthorityAction = ();

        fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
            action
        }

        fn accept_seq(&self) -> u64 {
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

        state1.update(&RecoverableStateAction::StateAction { action: () });
        state2.update(&RecoverableStateAction::StateAction { action: () });
        state1.update(&RecoverableStateAction::StateAction { action: () });
        state2.update(&RecoverableStateAction::StateAction { action: () });

        state1.update(&RecoverableStateAction::BumpGeneration { new_id: 1234 });
        state1.update(&RecoverableStateAction::StateAction { action: () });
        state1.update(&RecoverableStateAction::StateAction { action: () });

        assert!(state1.details.can_recover_follower(&state2.details));

        state2.update(&RecoverableStateAction::BumpGeneration { new_id: 1234 });
        state2.update(&RecoverableStateAction::StateAction { action: () });
        state2.update(&RecoverableStateAction::StateAction { action: () });

        assert_eq!(state1, state2);
    }

    #[test]
    fn recoverable_state_encoding_test() {
        test_assert_valid_encoding(RecoverableState {
            details: RecoverableStateDetails {
                id: 32145342,
                generation: 2,
                outer_state_next_seq: 3213,
                inner_state_next_seq: 321,
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
                ]
                .into(),
            },
            state: MockState(300),
        });
    }

    #[test]
    fn recov_generation_end_encoding_test() {
        test_assert_valid_encoding(RecovGenerationEnd {
            old_id: 1241,
            generation: 8,
            next_sequence: 200,
        });
    }

    #[test]
    fn can_recover_follower_test() {
        let mut details = RecoverableStateDetails::new(123, 1);
        for seq in 1..30 {
            details.apply_inner_state_seq(seq);
        }

        {
            let mut fork = details.clone();

            details.apply_generation_bump(444);
            assert!(details.can_recover_follower(&fork));

            fork.apply_inner_state_seq(30);
            assert!(!details.can_recover_follower(&fork));
        }

        let fork = details.clone();

        for seq in 30..100 {
            if seq % 10 == 0 {
                details.apply_generation_bump(seq);
            }
            details.apply_inner_state_seq(seq);
        }

        assert!(details.can_recover_follower(&fork));

        {
            let mut fork = fork.clone();
            fork.apply_generation_bump(555);
            assert!(!details.can_recover_follower(&fork));
        }

        {
            let mut fork = fork.clone();
            fork.apply_inner_state_seq(30);
            assert!(!details.can_recover_follower(&fork));
        }
    }
}
