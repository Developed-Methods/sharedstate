use std::{collections::VecDeque, ops::{Deref, DerefMut}, sync::{atomic::{AtomicUsize, Ordering}, Arc, RwLock, RwLockReadGuard, TryLockError}};

use crate::utils::PanicHelper;

pub trait DeterministicState: Sized + Send + Sync + Clone + 'static {
    type Action: Sized + Send + Sync + 'static;
    type AuthorityAction: Sized + Send + Sync + 'static;

    fn id(&self) -> u64;

    fn sequence(&self) -> u64;

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction;

    fn update(&mut self, action: &Self::AuthorityAction);
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
                RwLock::new(state.clone()),
            ]
        });

        let shared = SharedState {
            inner: inner.clone(),
        };

        let updater = StateUpdater {
            inner,
            queue: VecDeque::new(),
            queue_offset: [0, 0],
            queue_next_sequence: sequence,
        };

        (
            shared,
            SharedStateUpdater::Leader(LeaderUpdater {
                state,
                updater,
            })
        )
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

pub enum SharedStateUpdater<D: DeterministicState> {
    Leader(LeaderUpdater<D>),
    Follower(FollowUpdater<D>),
}

impl<D: DeterministicState> SharedStateUpdater<D> {
    pub fn next_sequence(&self) -> u64 {
        match self {
            Self::Leader(l) => {
                let v = l.updater.queue_next_sequence;
                assert_eq!(v, l.state.sequence());
                v
            },
            Self::Follower(f) => f.updater.queue_next_sequence,
        }
    }

    pub fn update(&mut self) -> bool {
        match self {
            Self::Leader(l) => l.updater.update(),
            Self::Follower(f) => f.updater.update(),
        }
    }

    pub fn into_follow(self) -> FollowUpdater<D> {
        match self {
            Self::Follower(f) => f,
            Self::Leader(l) => l.follow(),
        }
    }

    pub fn into_lead(self) -> LeaderUpdater<D> {
        match self {
            Self::Leader(l) => l,
            Self::Follower(f) => f.lead(),
        }
    }

    pub fn as_leader(&mut self) -> Option<&mut LeaderUpdater<D>> {
        match self {
            Self::Leader(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_follower(&mut self) -> Option<&mut LeaderUpdater<D>> {
        match self {
            Self::Leader(l) => Some(l),
            _ => None,
        }
    }
}

pub struct LeaderUpdater<D: DeterministicState> {
    updater: StateUpdater<D>,
    state: D,
}

impl<D: DeterministicState> LeaderUpdater<D> {
    pub fn queue(&mut self, action: D::Action) -> (u64, &D::AuthorityAction) {
        let authority = self.state.authority(action);

        let seq = self.state.sequence();
        self.state.update(&authority);
        self.updater.queue_sequenced(seq, authority).panic("invalid sequence")
    }

    pub fn update_ready(&mut self) -> bool {
        self.updater.update_ready()
    }

    pub fn update(&mut self) {
        self.updater.update();
    }

    pub fn flush(&mut self) {
        self.updater.flush_queue();
    }

    pub fn state(&self) -> &D {
        &self.state
    }

    pub fn update_state<F: FnOnce(&mut StatePtr<D>)>(&mut self, update: F) {
        let mut ptr = StatePtr {
            item: &mut self.state,
            as_mut: false
        };

        update(&mut ptr);

        if ptr.as_mut {
            self.updater.reset(self.state.clone());
        }
    }

    pub fn next_sequence(&self) -> u64 {
        self.updater.queue_next_sequence
    }

    pub fn follow(self) -> FollowUpdater<D> {
        FollowUpdater { updater: self.updater }
    }
}

pub struct StatePtr<'a, T> {
    item: &'a mut T,
    as_mut: bool,
}

impl<'a, T> AsRef<T> for StatePtr<'a, T> {
    fn as_ref(&self) -> &T {
        self.item
    }
}

impl<'a, T> Deref for StatePtr<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.item
    }
}

impl<'a, T> AsMut<T> for StatePtr<'a, T> {
    fn as_mut(&mut self) -> &mut T {
        self.as_mut = true;
        self.item
    }
}

impl<'a, T> DerefMut for StatePtr<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut = true;
        self.item
    }
}

pub struct FollowUpdater<D: DeterministicState> {
    updater: StateUpdater<D>,
}

impl<D: DeterministicState> FollowUpdater<D> {
    pub fn reset(&mut self, state: D) {
        self.updater.reset(state);
    }

    pub fn queue(&mut self, sequence: u64, action: D::AuthorityAction) -> bool {
        self.updater.queue_sequenced(sequence, action).is_ok()
    }

    pub fn update(&mut self) {
        self.updater.update();
    }

    pub fn flush(&mut self) {
        self.updater.flush_queue();
    }

    pub fn read_state(&self) -> RwLockReadGuard<D> {
        self.updater.inner.states[0].read().panic("failed to read lock")
    }

    pub fn next_sequence(&self) -> u64 {
        self.updater.queue_next_sequence
    }

    pub fn lead(mut self) -> LeaderUpdater<D> {
        self.updater.flush_queue();

        let state = {
            let state0 = self.updater.inner.states[0].read().panic("failed to read lock state");
            let state1 = self.updater.inner.states[1].read().panic("failed to read lock state");
            assert_eq!(state0.sequence(), state1.sequence());
            state0.clone()
        };

        LeaderUpdater {
            state,
            updater: self.updater,
        }
    }
}

pub struct StateUpdater<D: DeterministicState> {
    inner: Arc<StateInner<D>>,
    queue: VecDeque<(u64, D::AuthorityAction)>,
    queue_next_sequence: u64,
    queue_offset: [usize; 2],
}

struct StateInner<D: DeterministicState> {
    read_pos: AtomicUsize,
    states: [RwLock<D>; 2],
}

impl<D: DeterministicState> StateUpdater<D> {
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

    pub fn queue_sequenced(&mut self, seq: u64, item: D::AuthorityAction) -> Result<(u64, &D::AuthorityAction), D::AuthorityAction> {
        if self.queue_next_sequence != seq {
            return Err(item);
        }
        Ok((seq, self.queue(item)))
    }

    pub fn queue(&mut self, item: D::AuthorityAction) -> &D::AuthorityAction {
        self.queue.push_back((self.queue_next_sequence, item));
        self.queue_next_sequence += 1;
        &self.queue.back().unwrap().1
    }

    pub fn next_queued_sequence(&self) -> u64 {
        self.queue_next_sequence
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
        let trim_size = self.queue_offset[0]
            .min(self.queue_offset[1]);

        for _ in 0..trim_size {
            let _ = self.queue.pop_front();
        }

        self.queue_offset[0] -= trim_size;
        self.queue_offset[1] -= trim_size;
    }
}

pub struct StateActionIter<'a, D: DeterministicState> {
    queue: &'a VecDeque<(u64, D::AuthorityAction)>,
    pos: usize,
}

impl<'a, D: DeterministicState> Iterator for StateActionIter<'a, D> {
    type Item = (u64, &'a D::AuthorityAction);

    fn next(&mut self) -> Option<Self::Item> {
        let (seq, value) = self.queue.get(self.pos)?;
        self.pos += 1;
        Some((*seq, value))
    }
}


#[cfg(test)]
mod test {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{DeterministicState, SharedState};

    #[derive(Clone, Debug, Default)]
    struct TestState {
        id: u64,
        sequence: u64,
        time: u64,
        numbers: Vec<u64>,
    }

    impl DeterministicState for TestState {
        type Action = u64;
        type AuthorityAction = (u64, u64);

        fn id(&self) -> u64 {
            self.id
        }

        fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
            (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64, action)
        }

        fn sequence(&self) -> u64 {
            self.sequence
        }

        fn update(&mut self, action: &Self::AuthorityAction) {
            self.sequence += 1;

            self.time = action.0;
            self.numbers.push(action.1);
        }
    }

    // #[test]
    // fn state_tests() {
    //     let (state, mut updater) = SharedState::new(TestState::default());
    //     updater = updater.lead();

    //     for i in 0..20 {
    //         updater.as_leader().unwrap().queue(i);
    //     }

    //     /* flush updates */
    //     let mut no_update_count = 0;
    //     loop {
    //         if !updater.update() {
    //             no_update_count += 1;
    //             if no_update_count <= 2 {
    //                 break;
    //             }
    //         } else {
    //             no_update_count = 0;
    //         }
    //     }

    //     assert!(updater.action_history(3).is_none());
    //     assert_eq!(updater.oldest_seq(), updater.action_history(10).unwrap().next().unwrap().0);

    //     let mut items = updater.action_history(10).unwrap();
    //     for i in 10..20 {
    //         assert_eq!(items.next().unwrap(), (i, &(0, i)));
    //     }
    // }
}
