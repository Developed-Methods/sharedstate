use std::{collections::VecDeque, ops::{Deref, DerefMut}, sync::{atomic::{AtomicUsize, Ordering}, Arc}};
use parking_lot::{RwLock, RwLockReadGuard};

use crate::utils::PanicHelper;

pub trait DeterministicState: Sized + Send + Sync + Clone + 'static {
    type Action: Sized + Send + Sync + 'static;
    type AuthorityAction: Sized + Send + Sync + 'static;

    fn accept_seq(&self) -> u64;

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction;

    fn update(&mut self, action: &Self::AuthorityAction);
}

pub struct SharedState<D: DeterministicState> {
    inner: Arc<StateInner<D>>,
}

impl<D: DeterministicState + Clone> SharedState<D> {
    pub fn new(state: D) -> (Self, FlushedUpdater<D>) {
        let accept_seq = state.accept_seq();

        let inner = Arc::new(StateInner {
            read_pos: AtomicUsize::new(0),
            states: [
                RwLock::new(state.clone()),
                RwLock::new(state),
            ]
        });

        let shared = SharedState {
            inner: inner.clone(),
        };

        let updater = StateUpdater {
            inner,
            queue: VecDeque::new(),
            queue_offset: [0, 0],
            queue_accept_seq: accept_seq,
        };

        (
            shared,
            FlushedUpdater {
                updater,
                state: None,
            }
        )
    }
}

pub struct FlushedUpdater<D: DeterministicState> {
    state: Option<D>,
    updater: StateUpdater<D>,
}

impl<D: DeterministicState> FlushedUpdater<D> {
    pub fn accept_seq(&self) -> u64 {
        self.updater.queue_accept_seq
    }

    pub fn reset_state(&mut self, state: D) {
        self.state = None;
        self.updater.reset(state);
    }

    pub fn view_state<R, F: FnOnce(&D) -> R>(&self, update: F) -> R {
        if let Some(state) = &self.state {
            update(state)
        } else {
            let state = self.updater.read();
            update(&*state)
        }
    }

    pub fn mutate_state<R, F: FnOnce(&mut StatePtr<D>) -> R>(&mut self, update: F) -> R {
        if let Some(state) = &mut self.state {
            let mut ptr = StatePtr {
                item: state,
                as_mut: false
            };

            let result = update(&mut ptr);

            if ptr.as_mut {
                self.updater.reset(state.clone());
            }

            result
        } else {
            let mut state = self.updater.read().clone();

            let mut ptr = StatePtr {
                item: &mut state,
                as_mut: false
            };

            let result = update(&mut ptr);

            if ptr.as_mut {
                self.updater.reset(state);
            }

            result
        }
    }

    pub fn into_lead(mut self) -> LeadUpdater<D> {
        let state = match self.state.take() {
            None => self.updater.read().clone(),
            Some(state) => state,
        };

        LeadUpdater {
            updater: self.updater,
            state,
        }
    }

    pub fn into_follow(self) -> FollowUpdater<D> {
        FollowUpdater {
            updater: self.updater,
        }
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
            if let Some(lock) = self.states[idx].try_read() {
                return lock;
            }
            std::hint::spin_loop();
        }

        /* should not happen as read */
        panic!("failed to acquire read lock");
    }
}

pub struct LeadUpdater<D: DeterministicState> {
    updater: StateUpdater<D>,
    state: D,
}

impl<D: DeterministicState> LeadUpdater<D> {
    pub fn queue(&mut self, action: D::Action) -> (u64, &D::AuthorityAction) {
        let authority = self.state.authority(action);

        let seq = self.state.accept_seq();
        self.state.update(&authority);
        self.updater.queue_sequenced(seq, authority).panic("invalid sequence")
    }

    pub fn update_ready(&mut self) -> bool {
        self.updater.update_ready()
    }

    pub fn update(&mut self) -> bool {
        self.updater.update()
    }

    pub fn flush(&mut self) {
        self.updater.flush_queue();
    }

    pub fn state(&self) -> &D {
        &self.state
    }

    pub fn accept_seq(&self) -> u64 {
        self.updater.queue_accept_seq
    }

    pub fn into_follow(self) -> FollowUpdater<D> {
        FollowUpdater { updater: self.updater }
    }

    pub fn into_flushed(mut self) -> FlushedUpdater<D> {
        self.updater.flush_queue();

        {
            let state0 = self.updater.inner.states[0].read();
            let state1 = self.updater.inner.states[1].read();
            assert_eq!(state0.accept_seq(), state1.accept_seq());
            assert_eq!(state0.accept_seq(), self.state.accept_seq());
        }

        FlushedUpdater {
            state: Some(self.state),
            updater: self.updater,
        }
    }
}

pub struct StatePtr<'a, T> {
    item: &'a mut T,
    as_mut: bool,
}

impl<T> AsRef<T> for StatePtr<'_, T> {
    fn as_ref(&self) -> &T {
        self.item
    }
}

impl<T> Deref for StatePtr<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.item
    }
}

impl<T> AsMut<T> for StatePtr<'_, T> {
    fn as_mut(&mut self) -> &mut T {
        self.as_mut = true;
        self.item
    }
}

impl<T> DerefMut for StatePtr<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut = true;
        self.item
    }
}

pub struct FollowUpdater<D: DeterministicState> {
    updater: StateUpdater<D>,
}

impl<D: DeterministicState> FollowUpdater<D> {
    pub fn queue(&mut self, seq: u64, action: D::AuthorityAction) -> &D::AuthorityAction {
        self.updater.queue_sequenced(seq, action).panic("invalid sequence").1
    }

    pub fn update_ready(&self) -> bool {
        self.updater.update_ready()
    }

    pub fn update(&mut self) -> bool {
        self.updater.update()
    }

    pub fn flush(&mut self) {
        self.updater.flush_queue();
    }

    pub fn read_state(&self) -> RwLockReadGuard<D> {
        self.updater.inner.states[0].read()
    }

    pub fn accept_seq(&self) -> u64 {
        self.updater.queue_accept_seq
    }

    pub fn into_flushed(mut self) -> FlushedUpdater<D> {
        self.updater.flush_queue();

        {
            let state0 = self.updater.inner.states[0].read();
            let state1 = self.updater.inner.states[1].read();
            assert_eq!(state0.accept_seq(), state1.accept_seq());
        }

        FlushedUpdater {
            state: None,
            updater: self.updater,
        }
    }

    pub fn into_lead(mut self) -> LeadUpdater<D> {
        self.updater.flush_queue();

        let state = {
            let state0 = self.updater.inner.states[0].read();
            let state1 = self.updater.inner.states[1].read();
            assert_eq!(state0.accept_seq(), state1.accept_seq());
            state0.clone()
        };

        LeadUpdater {
            state,
            updater: self.updater,
        }
    }
}

pub struct StateUpdater<D: DeterministicState> {
    inner: Arc<StateInner<D>>,
    queue: VecDeque<(u64, D::AuthorityAction)>,
    queue_accept_seq: u64,
    queue_offset: [usize; 2],
}

struct StateInner<D: DeterministicState> {
    read_pos: AtomicUsize,
    states: [RwLock<D>; 2],
}

impl<D: DeterministicState> StateUpdater<D> {
    pub fn shared(&self) -> SharedState<D> {
        SharedState { inner: self.inner.clone() }
    }

    pub fn reset(&mut self, state: D) {
        /* we're the only writer so load can be relaxed */
        let read_pos = self.inner.read_pos.load(Ordering::Relaxed);

        /* write pos shouldn't be locked or locks should be dropping soon */
        let write_pos = read_pos.overflowing_add(1).0;
        let write_idx = write_pos & 0x1;

        let state_sequence = state.accept_seq();

        {
            let mut state_lock = self.inner.states[write_idx].write();
            *state_lock = state.clone();
        }

        self.inner.read_pos.store(write_pos, Ordering::Release);

        {
            let mut state_lock = self.inner.states[read_pos & 0x1].write();
            *state_lock = state;
        }

        self.queue.clear();
        self.queue_offset = [0, 0];
        self.queue_accept_seq = state_sequence;
    }

    pub fn queue_sequenced(&mut self, seq: u64, item: D::AuthorityAction) -> Result<(u64, &D::AuthorityAction), D::AuthorityAction> {
        if self.queue_accept_seq != seq {
            return Err(item);
        }
        Ok((seq, self.queue(item)))
    }

    pub fn queue(&mut self, item: D::AuthorityAction) -> &D::AuthorityAction {
        self.queue.push_back((self.queue_accept_seq, item));
        self.queue_accept_seq += 1;
        &self.queue.back().unwrap().1
    }

    pub fn next_queued_sequence(&self) -> u64 {
        self.queue_accept_seq
    }

    pub fn update_ready(&self) -> bool {
        let read_pos = self.inner.read_pos.load(Ordering::Relaxed);
        let write_pos = read_pos.overflowing_add(1).0;
        self.inner.states[write_pos & 0x1].try_write().is_some()
    }

    pub fn flush_queue(&mut self) {
        self.update();
        self.update();

        if self.update() {
            panic!("expected update to be flushed after 2 .update() calls");
        }

        assert_eq!(self.queue.len(), 0);
    }

    pub fn read(&self) -> RwLockReadGuard<D> {
        self.inner.read()
    }

    pub fn update(&mut self) -> bool {
        /* we're the only writer so load can be relaxed */
        let read_pos = self.inner.read_pos.load(Ordering::Acquire);

        /* write pos shouldn't be locked or locks should be dropping soon */
        let write_pos = read_pos.overflowing_add(1).0;
        let write_idx = write_pos & 0x1;

        let mut had_update = false;

        /* apply all actions available in the queue */
        {
            let mut state = self.inner.states[write_idx].write();
            let mut offset = self.queue_offset[write_idx];

            let mut next_seq = state.accept_seq() + 1;
            while offset < self.queue.len() {
                let (target_seq, action) = &self.queue[offset];
                assert_eq!(state.accept_seq(), *target_seq);
                offset += 1;

                had_update = true;
                state.update(action);

                assert_eq!(state.accept_seq(), next_seq, "state::update(action) did not increment sequence");
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
        let trim_size = self.queue_offset[0]
            .min(self.queue_offset[1]);

        for _ in 0..trim_size {
            let _ = self.queue.pop_front();
        }

        self.queue_offset[0] -= trim_size;
        self.queue_offset[1] -= trim_size;
    }
}

#[cfg(test)]
mod test {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct TestState {
        accept_seq: u64,
        time: u64,
        numbers: Vec<u64>,
    }

    impl DeterministicState for TestState {
        type Action = u64;
        type AuthorityAction = (u64, u64);

        fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
            (
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
                action
            )
        }

        fn accept_seq(&self) -> u64 {
            self.accept_seq
        }

        fn update(&mut self, action: &Self::AuthorityAction) {
            self.accept_seq += 1;

            self.time = action.0;
            self.numbers.push(action.1);
        }
    }

    #[test]
    fn state_flush_tests() {
        let (state, updater) = SharedState::new(TestState::default());
        let mut updater = updater.into_lead();

        assert_eq!(state.read().accept_seq, 0);
        assert_eq!(updater.accept_seq(), 0);

        for i in 0..20 {
            updater.queue(i);
        }

        updater.flush();

        {
            assert_eq!(updater.accept_seq(), 20);
            let read = state.read();
            assert_eq!(read.numbers.len(), 20);
        }

        let mut updater = updater.into_flushed();
        updater.mutate_state(|state| {
            state.numbers.clear();
            state.numbers.push(1);
            state.accept_seq = 1000;
        });

        {
            assert_eq!(updater.accept_seq(), 1000);
            let read = state.read();
            assert_eq!(read.numbers.len(), 1);
        }
    }
}
