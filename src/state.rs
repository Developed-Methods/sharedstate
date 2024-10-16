use std::{collections::VecDeque, sync::{atomic::{AtomicUsize, Ordering}, Arc, RwLock, RwLockReadGuard, TryLockError}};

pub trait DeterministicState: Sized + Send + Sync + Clone + 'static {
    type Action: Sized + Send + Sync + 'static;

    fn id(&self) -> u64;

    fn sequence(&self) -> u64;

    fn update(&mut self, action: &Self::Action);
}

pub struct SharedState<D: DeterministicState> {
    inner: Arc<StateInner<D>>,
}

impl<D: DeterministicState + Clone> SharedState<D> {
    pub fn new(state: D) -> (Self, SharedStateUpdater<D>) {
        let sequence = state.sequence();

        let inner = Arc::new(StateInner {
            read_pos: AtomicUsize::new(0),
            states: [
                RwLock::new(state.clone()),
                RwLock::new(state),
            ]
        });

        let state = SharedState {
            inner: inner.clone(),
        };

        let updater = SharedStateUpdater {
            inner,
            queue: VecDeque::new(),
            queue_offset: [0, 0],
            queue_next_sequence: sequence,
            history_count: 0,
        };

        (state, updater)
    }
}

impl<D: DeterministicState> Clone for SharedState<D> {
    fn clone(&self) -> Self {
        SharedState { inner: self.inner.clone() }
    }
}

impl<D: DeterministicState> SharedState<D> {
    pub fn read(&self) -> RwLockReadGuard<D> {
        self.inner.read()
    }
}

impl<D: DeterministicState> StateInner<D> {
    pub fn read(&self) -> RwLockReadGuard<D> {
        for _ in 0..1048576 {
            let pos = self.read_pos.load(Ordering::Acquire);
            let idx = pos & 0x1;
            if let Ok(lock) = self.states[idx].try_read() {
                return lock;
            }
            std::hint::spin_loop();
        }

        /* should not happen as read */
        panic!("failed to acquire read lock");
    }
}

pub struct SharedStateUpdater<D: DeterministicState> {
    inner: Arc<StateInner<D>>,
    queue: VecDeque<(u64, D::Action)>,
    queue_next_sequence: u64,
    queue_offset: [usize; 2],
    history_count: usize,
}

struct StateInner<D: DeterministicState> {
    read_pos: AtomicUsize,
    states: [RwLock<D>; 2],
}

impl<D: DeterministicState> SharedStateUpdater<D> {
    pub fn shared(&self) -> SharedState<D> {
        SharedState {
            inner: self.inner.clone(),
        }
    }

    pub fn reset(&mut self, state: D) {
        /* we're the only writer so load can be relaxed */
        let read_pos = self.inner.read_pos.load(Ordering::Relaxed);

        /* write pos shouldn't be locked or locks should be dropping soon */
        let write_pos = read_pos.overflowing_add(1).0;
        let write_idx = write_pos & 0x1;

        let state_sequence = state.sequence();

        {
            let mut state_lock = self.inner.states[write_idx].write().unwrap();
            *state_lock = state.clone();
        }

        self.inner.read_pos.store(write_pos, Ordering::Release);

        {
            let mut state_lock = self.inner.states[read_pos & 0x1].write().unwrap();
            *state_lock = state;
        }

        self.queue.clear();
        self.queue_offset = [0, 0];
        self.queue_next_sequence = state_sequence;
    }

    pub fn queue_sequenced(&mut self, seq: u64, item: D::Action) -> Result<(), D::Action> {
        if self.queue_next_sequence != seq {
            return Err(item);
        }
        self.queue(item);
        Ok(())
    }

    pub fn next_queued_sequence(&self) -> u64 {
        self.queue_next_sequence
    }

    pub fn queue(&mut self, item: D::Action) {
        self.queue.push_back((self.queue_next_sequence, item));
        self.queue_next_sequence += 1;
    }

    pub fn set_history_count(&mut self, count: usize) {
        self.history_count = count;
    }

    pub fn history_count(&self) -> usize {
        self.history_count
    }

    pub fn oldest_seq(&self) -> u64 {
        let count = self.history_count as u64;
        self.queue_next_sequence.max(count) - count
    }

    pub fn action_history(&self, sequence: u64) -> Option<StateActionIter<D>> {
        if self.queue_next_sequence <= sequence {
            return None;
        }
        let depth = (self.queue_next_sequence - sequence) as usize;
        if self.queue.len() < depth {
            return None;
        }

        Some(StateActionIter {
            pos: self.queue.len() - depth,
            queue: &self.queue,
        })
    }

    pub fn update_ready(&self) -> bool {
        let read_pos = self.inner.read_pos.load(Ordering::Relaxed);
        let write_pos = read_pos.overflowing_add(1).0;

        match self.inner.states[write_pos & 0x1].try_write() {
            Ok(_) => true,
            Err(TryLockError::WouldBlock) => false,
            Err(TryLockError::Poisoned(p)) => panic!("state lock poinsoned: {:?}", p),
        }
    }

    pub fn flush_queue(&mut self) {
        let mut done_count = 0;

        for _ in 0..10 {
            if self.update() {
                done_count = 0;
                continue;
            }

            done_count += 1;
            if 2 < done_count  {
                return;
            }
        }

        panic!("expected update to be flushed after 2 .update() calls, did 10");
    }

    pub fn read(&self) -> RwLockReadGuard<D> {
        self.inner.read()
    }

    pub fn update(&mut self) -> bool {
        /* we're the only writer so load can be relaxed */
        let read_pos = self.inner.read_pos.load(Ordering::Relaxed);

        /* write pos shouldn't be locked or locks should be dropping soon */
        let write_pos = read_pos.overflowing_add(1).0;
        let write_idx = write_pos & 0x1;

        let mut had_update = false;

        /* apply all actions available in the queue */
        {
            let mut state = self.inner.states[write_idx].write().unwrap();
            let mut offset = self.queue_offset[write_idx];

            let mut next_seq = state.sequence() + 1;
            while offset < self.queue.len() {
                let (target_seq, action) = &self.queue[offset];
                assert_eq!(state.sequence(), *target_seq);
                offset += 1;

                had_update = true;
                state.update(action);

                assert_eq!(state.sequence(), next_seq, "state::update(action) did not increment sequence");
                next_seq += 1;
            }

            self.queue_offset[write_idx] = offset;
        }

        self.trim_queue();

        /* set reader to be updated pos so current reader can be updated */
        self.inner.read_pos.store(write_pos, Ordering::Release);

        had_update
    }

    fn trim_queue(&mut self) {
        let max_remove = self.queue.len().max(self.history_count) - self.history_count;

        let trim_size = self.queue_offset[0]
            .min(self.queue_offset[1])
            .min(max_remove);

        for _ in 0..trim_size {
            let _ = self.queue.pop_front();
        }

        self.queue_offset[0] -= trim_size;
        self.queue_offset[1] -= trim_size;
    }
}

pub struct StateActionIter<'a, D: DeterministicState> {
    queue: &'a VecDeque<(u64, D::Action)>,
    pos: usize,
}

impl<'a, D: DeterministicState> Iterator for StateActionIter<'a, D> {
    type Item = (u64, &'a D::Action);

    fn next(&mut self) -> Option<Self::Item> {
        let (seq, value) = self.queue.get(self.pos)?;
        self.pos += 1;
        Some((*seq, value))
    }
}


#[cfg(test)]
mod test {
    use super::{DeterministicState, SharedState};

    #[derive(Clone, Debug, Default)]
    struct TestState {
        id: u64,
        sequence: u64,
        numbers: Vec<u64>,
    }

    impl DeterministicState for TestState {
        type Action = u64;

        fn id(&self) -> u64 {
            self.id
        }

        fn sequence(&self) -> u64 {
            self.sequence
        }

        fn update(&mut self, action: &Self::Action) {
            self.sequence += 1;
            self.numbers.push(*action);
        }
    }

    #[test]
    fn test_depth() {
        let (state, mut updater) = SharedState::new(TestState::default());
        updater.set_history_count(10);

        for i in 0..20 {
            updater.queue(i);
        }

        /* flush updates */
        let mut no_update_count = 0;
        loop {
            if !updater.update() {
                no_update_count += 1;
                if no_update_count <= 2 {
                    break;
                }
            } else {
                no_update_count = 0;
            }
        }

        assert!(updater.action_history(3).is_none());
        assert_eq!(updater.oldest_seq(), updater.action_history(10).unwrap().next().unwrap().0);

        let mut items = updater.action_history(10).unwrap();
        for i in 10..20 {
            assert_eq!(items.next().unwrap(), (i, &i));
        }
    }
}
