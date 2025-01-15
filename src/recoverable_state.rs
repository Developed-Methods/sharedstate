use std::collections::VecDeque;
use message_encoding::{m_opt_sum, m_static, MessageEncoding};
use crate::state::DeterministicState;


#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoverableState<D: DeterministicState> {
    generation: u64,
    history: VecDeque<RecovGenerationEnd>,
    state: D,
}

impl<D: DeterministicState + Default> Default for RecoverableState<D> {
    fn default() -> Self {
        Self::first_generation(D::default())
    }
}

pub trait StateReplaceCheck<T>: Send + Sync + 'static {
    fn should_replace_state(&self, current: &T, option_id: u64, option_gen: u64, option_seq: u64) -> bool;

    fn verify_replace_state(&self, current: &T, option: &T) -> bool;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecovGenerationEnd {
    pub generation: u64,
    pub next_sequence: u64,
}

impl<D: DeterministicState> RecoverableState<D> {
    pub fn first_generation(state: D) -> Self {
        RecoverableState {
            generation: 1,
            history: VecDeque::with_capacity(128),
            state,
        }
    }

    pub fn generations(&self) -> Vec<RecovGenerationEnd> {
        self.history.iter().cloned().collect()
    }

    pub fn set_generations(&mut self, generations: Vec<RecovGenerationEnd>) -> Result<(), &'static str> {
        if generations.is_empty() {
            if !self.history.is_empty() {
                return Err("cannot clear generation history");
            }
            return Ok(());
        }

        let mut last = generations.first().unwrap().clone();
        for gen in generations.iter().skip(1) {
            if gen.generation != last.generation + 1 {
                return Err("generations are not sequential");
            }
            if gen.next_sequence < last.next_sequence {
                return Err("sequence is decreasing");
            }
            last = gen.clone();
        }

        self.history = generations.into_iter().collect();
        self.generation = self.history.back().unwrap().generation + 1;

        Ok(())
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn bump_generation(&mut self) {
        if 2048 <= self.history.len() {
            let _ = self.history.pop_back();
        }

        self.history.push_back(RecovGenerationEnd {
            generation: self.generation,
            next_sequence: self.state.sequence(),
        });

        self.generation += 1;
    }

    pub fn is_recoverable(&self, id: u64, gen: u64, seq: u64) -> bool {
        if self.id() != id {
            return false;
        }

        if self.generation < gen {
            return false;
        }

        if self.generation == gen {
            return seq <= self.sequence();
        }

        let depth = (self.generation - gen) as usize;
        if self.history.len() < depth {
            return false;
        }

        let hist = self.history.get(self.history.len() - depth).unwrap();
        assert_eq!(hist.generation, gen);

        seq <= hist.next_sequence
    }

    pub fn state(&self) -> &D {
        &self.state
    }
}

impl<D: DeterministicState> DeterministicState for RecoverableState<D> {
    type Action = D::Action;
    type AuthorityAction = D::AuthorityAction;

    fn id(&self) -> u64 {
        self.state.id()
    }

    fn sequence(&self) -> u64 {
        self.state.sequence()
    }

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
        self.state.authority(action)
    }

    fn update(&mut self, action: &Self::AuthorityAction) {
        self.state.update(action)
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
        sum += self.generation.write_to(out)?;
        sum += (self.history.len() as u64).write_to(out)?;
        for hist in &self.history {
            sum += hist.generation.write_to(out)?;
            sum += hist.next_sequence.write_to(out)?;
        }
        sum += self.state.write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(RecoverableState {
            generation: MessageEncoding::read_from(read)?,
            history: {
                let len = u64::read_from(read)? as usize;
                let mut vec = VecDeque::with_capacity(len);
                for _ in 0..len {
                    vec.push_back(RecovGenerationEnd {
                        generation: MessageEncoding::read_from(read)?,
                        next_sequence: MessageEncoding::read_from(read)?,
                    });
                }
                vec
            },
            state: MessageEncoding::read_from(read)?,
        })
    }
}

#[cfg(test)]
mod test {
    use message_encoding::{test_assert_valid_encoding, MessageEncoding};

    use crate::state::DeterministicState;

    use super::{RecovGenerationEnd, RecoverableState};

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct MockState(u64);

    impl DeterministicState for MockState {
        type Action = ();
        type AuthorityAction = ();

        fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
            action
        }

        fn id(&self) -> u64 {
            1
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
    fn recoverable_state_encoding_test() {
        test_assert_valid_encoding(RecoverableState {
            generation: 10,
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
            state: MockState(300),
        })
    }
}
