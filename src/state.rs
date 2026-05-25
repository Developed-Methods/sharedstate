use std::{
    fmt,
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::Arc,
};

use hotread::{HotRead, HotReadHandle, HotReadState};

pub trait DeterministicState: Sized + Send + Sync + Clone + 'static {
    type Action: Sized + Send + Sync + 'static;
    type AuthorityAction: Sized + Clone + Send + Sync + 'static;

    fn accept_seq(&self) -> u64;

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction;

    fn update(&mut self, action: &Self::AuthorityAction);
}

#[derive(Clone)]
struct HotSharedState<D: DeterministicState> {
    state: D,
}

#[derive(Clone)]
enum HotSharedStateUpdate<D: DeterministicState> {
    Authority {
        seq: u64,
        action: D::AuthorityAction,
    },
    Reset(D),
}

impl<D: DeterministicState> HotReadState for HotSharedState<D> {
    type Action = HotSharedStateUpdate<D>;

    fn apply_update(&mut self, update: &Self::Action) {
        match update {
            HotSharedStateUpdate::Authority { seq, action } => {
                assert_eq!(self.state.accept_seq(), *seq);
                self.state.update(action);
                assert_eq!(self.state.accept_seq(), seq + 1);
            }
            HotSharedStateUpdate::Reset(state) => {
                self.state = state.clone();
            }
        }
    }
}

struct StateInner<D: DeterministicState> {
    hot: HotRead<HotSharedState<D>>,
}

impl<D: DeterministicState> StateInner<D> {
    fn maintain(&self) -> bool {
        let result = self.hot.maintain();

        result.applied_updates != 0
            || result.pruned_updates != 0
            || result.ready_to_publish
            || result.blocked_by_workers
    }

    fn maintain_until_flushed(&self) {
        const MAX_SPINS: usize = 1_048_576;

        for _ in 0..MAX_SPINS {
            if self.hot.queued_update_count() == 0 {
                return;
            }

            let result = self.hot.maintain();

            if result.blocked_by_workers {
                std::hint::spin_loop();
            }
        }

        panic!(
            "failed to flush shared state: stale reader handles are blocking hotread maintenance"
        );
    }

    fn read(&self) -> SharedStateReadGuard<'_, D> {
        let mut handle = self.hot.create_handle();
        let state = NonNull::from(&handle.current().state);

        SharedStateReadGuard { handle, state }
    }

    fn current_state(&self) -> D {
        self.read().clone()
    }
}

pub struct SharedState<D: DeterministicState> {
    inner: Arc<StateInner<D>>,
}

impl<D: DeterministicState> SharedState<D> {
    pub fn new(state: D) -> (Self, FlushedUpdater<D>) {
        let accept_seq = state.accept_seq();

        let inner = Arc::new(StateInner {
            hot: HotRead::new(HotSharedState { state }),
        });

        let shared = SharedState {
            inner: inner.clone(),
        };

        (
            shared,
            FlushedUpdater {
                state: None,
                inner,
                accept_seq,
            },
        )
    }

    pub fn reader(&self) -> SharedStateReader<'_, D> {
        SharedStateReader {
            handle: self.inner.hot.create_handle(),
        }
    }

    pub fn read(&self) -> SharedStateReadGuard<'_, D> {
        self.inner.read()
    }
}

impl<D: DeterministicState> Clone for SharedState<D> {
    fn clone(&self) -> Self {
        SharedState {
            inner: self.inner.clone(),
        }
    }
}

pub struct SharedStateReader<'a, D: DeterministicState> {
    handle: HotReadHandle<'a, HotSharedState<D>>,
}

impl<D: DeterministicState> SharedStateReader<'_, D> {
    pub fn current(&mut self) -> &D {
        &self.handle.current().state
    }

    pub fn quiescent(&mut self) {
        self.handle.quiescent();
    }
}

pub struct SharedStateReadGuard<'a, D: DeterministicState> {
    handle: HotReadHandle<'a, HotSharedState<D>>,
    state: NonNull<D>,
}

impl<D: DeterministicState> Deref for SharedStateReadGuard<'_, D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        let _keep_handle_alive = &self.handle;

        // SAFETY: `state` points at the hotread copy selected by `handle.current()`.
        // The handle remains live for the lifetime of this guard, so hotread will not
        // mutate or recycle that copy until the guard is dropped.
        unsafe { self.state.as_ref() }
    }
}

impl<D> fmt::Debug for SharedStateReadGuard<'_, D>
where
    D: DeterministicState + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

pub struct FlushedUpdater<D: DeterministicState> {
    state: Option<D>,
    inner: Arc<StateInner<D>>,
    accept_seq: u64,
}

impl<D: DeterministicState> FlushedUpdater<D> {
    pub fn accept_seq(&self) -> u64 {
        self.accept_seq
    }

    pub fn reset_state(&mut self, state: D) {
        self.accept_seq = state.accept_seq();
        self.state = None;
        self.inner
            .hot
            .queue_update(HotSharedStateUpdate::Reset(state));
        self.inner.maintain_until_flushed();
    }

    pub fn view_state<R, F: FnOnce(&D) -> R>(&self, update: F) -> R {
        if let Some(state) = &self.state {
            update(state)
        } else {
            let state = self.inner.read();
            update(&state)
        }
    }

    pub fn mutate_state<R, F: FnOnce(&mut StatePtr<D>) -> R>(&mut self, update: F) -> R {
        if let Some(state) = &mut self.state {
            let mut ptr = StatePtr {
                item: state,
                as_mut: false,
            };

            let result = update(&mut ptr);

            if ptr.as_mut {
                self.accept_seq = state.accept_seq();
                self.inner
                    .hot
                    .queue_update(HotSharedStateUpdate::Reset(state.clone()));
                self.inner.maintain_until_flushed();
            }

            result
        } else {
            let mut state = self.inner.current_state();

            let mut ptr = StatePtr {
                item: &mut state,
                as_mut: false,
            };

            let result = update(&mut ptr);

            if ptr.as_mut {
                self.reset_state(state);
            }

            result
        }
    }

    pub fn into_lead(mut self) -> LeadUpdater<D> {
        let state = match self.state.take() {
            None => self.inner.current_state(),
            Some(state) => state,
        };

        LeadUpdater {
            inner: self.inner,
            accept_seq: self.accept_seq,
            state,
        }
    }

    pub fn into_follow(self) -> FollowUpdater<D> {
        FollowUpdater {
            inner: self.inner,
            accept_seq: self.accept_seq,
        }
    }
}

pub struct LeadUpdater<D: DeterministicState> {
    inner: Arc<StateInner<D>>,
    state: D,
    accept_seq: u64,
}

impl<D: DeterministicState> LeadUpdater<D> {
    pub fn queue(&mut self, action: D::Action) -> (u64, D::AuthorityAction) {
        let authority = self.state.authority(action);
        let seq = self.state.accept_seq();

        assert_eq!(self.accept_seq, seq);

        self.state.update(&authority);
        self.accept_seq = self.state.accept_seq();

        self.inner
            .hot
            .queue_update(HotSharedStateUpdate::Authority {
                seq,
                action: authority.clone(),
            });

        (seq, authority)
    }

    pub fn update_ready(&mut self) -> bool {
        self.inner.hot.queued_update_count() > 0
    }

    pub fn update(&mut self) -> bool {
        self.inner.maintain()
    }

    pub fn flush(&mut self) {
        self.inner.maintain_until_flushed();
    }

    pub fn state(&self) -> &D {
        &self.state
    }

    pub fn accept_seq(&self) -> u64 {
        self.accept_seq
    }

    pub fn into_follow(self) -> FollowUpdater<D> {
        self.inner.maintain_until_flushed();

        FollowUpdater {
            inner: self.inner,
            accept_seq: self.accept_seq,
        }
    }

    pub fn into_flushed(self) -> FlushedUpdater<D> {
        self.inner.maintain_until_flushed();

        {
            let state = self.inner.read();
            assert_eq!(state.accept_seq(), self.state.accept_seq());
        }

        FlushedUpdater {
            state: Some(self.state),
            inner: self.inner,
            accept_seq: self.accept_seq,
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
    inner: Arc<StateInner<D>>,
    accept_seq: u64,
}

impl<D: DeterministicState> FollowUpdater<D> {
    pub fn queue(&mut self, seq: u64, action: D::AuthorityAction) -> D::AuthorityAction {
        assert_eq!(self.accept_seq, seq);

        self.inner
            .hot
            .queue_update(HotSharedStateUpdate::Authority {
                seq,
                action: action.clone(),
            });

        self.accept_seq += 1;
        action
    }

    pub fn update_ready(&self) -> bool {
        self.inner.hot.queued_update_count() > 0
    }

    pub fn update(&mut self) -> bool {
        self.inner.maintain()
    }

    pub fn flush(&mut self) {
        self.inner.maintain_until_flushed();
    }

    pub fn read(&self) -> SharedStateReadGuard<'_, D> {
        self.inner.read()
    }

    pub fn read_state(&self) -> SharedStateReadGuard<'_, D> {
        self.read()
    }

    pub fn accept_seq(&self) -> u64 {
        self.accept_seq
    }

    pub fn into_flushed(self) -> FlushedUpdater<D> {
        self.inner.maintain_until_flushed();

        FlushedUpdater {
            state: None,
            inner: self.inner,
            accept_seq: self.accept_seq,
        }
    }

    pub fn into_lead(self) -> LeadUpdater<D> {
        self.inner.maintain_until_flushed();

        let state = self.inner.current_state();

        LeadUpdater {
            state,
            inner: self.inner,
            accept_seq: self.accept_seq,
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc,
        },
        time::{SystemTime, UNIX_EPOCH},
    };

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
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                action,
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

    #[test]
    fn reader_handle_sees_updates() {
        let (state, updater) = SharedState::new(TestState::default());
        let mut updater = updater.into_lead();
        let mut reader = state.reader();

        updater.queue(10);
        updater.queue(20);
        updater.flush();

        let current = reader.current();
        assert_eq!(current.accept_seq, 2);
        assert_eq!(current.numbers, vec![10, 20]);
    }

    #[test]
    fn reader_handle_can_be_used_from_thread() {
        let (state, updater) = SharedState::new(TestState::default());
        let mut updater = updater.into_lead();
        let run = Arc::new(AtomicBool::new(true));
        let latest = Arc::new(AtomicU64::new(0));

        let thread = std::thread::spawn({
            let state = state.clone();
            let run = run.clone();
            let latest = latest.clone();

            move || {
                let mut reader = state.reader();
                let mut last = 0;

                while run.load(Ordering::Acquire) {
                    let seq = reader.current().accept_seq;
                    assert!(last <= seq);
                    last = seq;
                    latest.store(seq, Ordering::Release);
                    std::hint::spin_loop();
                }

                reader.quiescent();
                last
            }
        });

        for i in 0..100 {
            updater.queue(i);
            updater.update();
        }
        updater.flush();

        while latest.load(Ordering::Acquire) < updater.accept_seq() {
            std::thread::yield_now();
        }

        run.store(false, Ordering::Release);
        let observed = thread.join().unwrap();

        assert_eq!(observed, updater.accept_seq());
    }

    #[test]
    fn follow_queue_applies_directly() {
        let (state, updater) = SharedState::new(TestState::default());
        let mut updater = updater.into_follow();

        updater.queue(0, (1, 42));
        updater.flush();

        let read = state.read();
        assert_eq!(read.accept_seq, 1);
        assert_eq!(read.numbers, vec![42]);
    }
}
