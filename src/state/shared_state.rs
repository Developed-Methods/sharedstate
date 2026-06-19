use std::sync::Arc;

use hotread::{HotRead, HotReadHandle, HotReadState};

use crate::state::determinstic_state::DeterministicState;

pub struct SharedState<D: DeterministicState> {
    inner: Arc<StateInner<D>>,
}

pub struct SharedStateReader<D: DeterministicState> {
    inner: Arc<StateInner<D>>,
}

impl<D: DeterministicState> Clone for SharedStateReader<D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub struct SharedStateHandle<D: DeterministicState> {
    handle: HotReadHandle<HotState<D>>,
}

impl<D: DeterministicState> SharedState<D> {
    pub fn new(state: D) -> Self {
        SharedState {
            inner: Arc::new(StateInner {
                hot: HotRead::new(HotState { state }),
            }),
        }
    }

    pub fn create_reader(&self) -> SharedStateReader<D> {
        SharedStateReader {
            inner: self.inner.clone(),
        }
    }

    pub fn create_handle(&self) -> SharedStateHandle<D> {
        SharedStateHandle {
            handle: self.inner.hot.create_handle(),
        }
    }

    pub fn queue_updates<I>(&mut self, updates: I)
    where
        I: IntoIterator<Item = (u64, D::AuthorityAction)>,
    {
        self.inner.hot.queue_updates(
            updates
                .into_iter()
                .map(|(seq, action)| HotSharedStateUpdate::Authority { seq, action }),
        );
    }

    pub fn reset(&mut self, state: D) {
        self.inner.hot.queue_update(HotSharedStateUpdate::Reset(state));
    }

    pub fn maintain_state(&self) {
        self.inner.hot.maintain();
    }
}

impl<D: DeterministicState> SharedStateReader<D> {
    pub fn create_handle(&self) -> SharedStateHandle<D> {
        SharedStateHandle {
            handle: self.inner.hot.create_handle(),
        }
    }
}

impl<D: DeterministicState> SharedStateHandle<D> {
    pub fn read(&mut self) -> &D {
        &self.handle.current().state
    }

    pub fn quiescent(&mut self) {
        self.handle.quiescent();
    }
}

struct StateInner<D: DeterministicState> {
    hot: HotRead<HotState<D>>,
}

#[derive(Clone)]
struct HotState<D: DeterministicState> {
    state: D,
}

#[derive(Clone)]
enum HotSharedStateUpdate<D: DeterministicState> {
    Authority { seq: u64, action: D::AuthorityAction },
    Reset(D),
}

impl<D: DeterministicState> HotReadState for HotState<D> {
    type Action = HotSharedStateUpdate<D>;

    fn apply_update(&mut self, update: &Self::Action) {
        match update {
            HotSharedStateUpdate::Authority { seq, action } => {
                if self.state.accept_seq() != *seq {
                    panic!("Received authority ")
                }
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
