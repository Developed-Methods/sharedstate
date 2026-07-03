use hotread::{HotRead, HotReadHandle, HotReadState};
use sequenced_broadcast::{
    SequencedBroadcast, SequencedBroadcastSettings, SequencedReceiver, SequencedSender, SettingsError, SubscribeError,
};
use tokio::sync::Mutex;

use crate::state::{
    deterministic_state::DeterministicState,
    recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails},
};

pub struct SubscribableState<D: DeterministicState> {
    state: HotRead<HotReadRecoverable<D>>,
    state_handle: Mutex<HotReadHandle<HotReadRecoverable<D>>>,
    broadcast: Mutex<SequencedBroadcast<RecoverableStateAction<D::AuthorityAction>>>,
    broadcast_sender: Mutex<SequencedSender<RecoverableStateAction<D::AuthorityAction>>>,
    broadcast_settings: SequencedBroadcastSettings,
}

impl<D: DeterministicState> SubscribableState<D> {
    pub fn new(state: RecoverableState<D>, settings: SequencedBroadcastSettings) -> Result<Self, SettingsError> {
        let (broadcast, broadcast_sender) = SequencedBroadcast::new(state.accept_seq(), settings.clone())?;

        let state = HotRead::new(HotReadRecoverable(state));
        let state_handle = Mutex::new(state.create_handle());

        Ok(SubscribableState {
            state,
            state_handle,
            broadcast: Mutex::new(broadcast),
            broadcast_sender: Mutex::new(broadcast_sender),
            broadcast_settings: settings,
        })
    }

    pub fn create_handle(&self) -> StateHandle<D> {
        StateHandle {
            handle: self.state.create_handle(),
        }
    }

    pub async fn recovery_details(&self) -> RecoverableStateDetails {
        let mut lock = self.state_handle.lock().await;
        let details = lock.current().0.details().clone();
        lock.quiescent();
        details
    }

    /// Recovery details once every queued update has been applied, so the
    /// reported next_seq matches the broadcast position. Blocks concurrent
    /// updates while it settles.
    pub async fn settled_recovery_details(&self) -> RecoverableStateDetails {
        let sender = self.broadcast_sender.lock().await;
        let target_seq = sender.seq();

        self.settle().await;

        let mut handle = self.state_handle.lock().await;
        loop {
            let details = handle.current().0.details().clone();
            if details.next_seq() == target_seq {
                handle.quiescent();
                return details;
            }
            handle.quiescent();
            self.state.maintain();
            tokio::task::yield_now().await;
        }
    }

    /// Drives hot-read maintenance until the published copy has every queued
    /// update. Publication can be deferred when a reader is mid-read, and
    /// nothing else retries it, so updates must settle before they can be
    /// relied on to be visible.
    async fn settle(&self) {
        loop {
            if self.state.copy_applied_seq(self.state.active_index()) == self.state.latest_seq() {
                return;
            }
            self.state.maintain();
            tokio::task::yield_now().await;
        }
    }

    pub async fn reset(&self, new_state: RecoverableState<D>) {
        let mut broadcast_locked = self.broadcast.lock().await;
        let mut sender_locked = self.broadcast_sender.lock().await;

        let new_recover_details = new_state.details().clone();

        self.state.queue_update(HotStateAction::Reset(new_state));
        self.settle().await;

        let (broadcast, broadcast_sender) =
            SequencedBroadcast::new(new_recover_details.next_seq(), self.broadcast_settings.clone()).unwrap();

        *broadcast_locked = broadcast;
        let mut old_sender = std::mem::replace(&mut *sender_locked, broadcast_sender);
        old_sender.close();
    }

    pub async fn subscribe(
        &self,
        recover: RecoverableStateDetails,
    ) -> Result<SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>, StateSubscribeError> {
        let broadcast_locked = self.broadcast.lock().await;

        let leader_details = {
            let mut lock = self.state_handle.lock().await;
            let details = lock.current().0.details().clone();
            lock.quiescent();
            details
        };

        if !leader_details.can_recover_follower(&recover) {
            return Err(StateSubscribeError::CannotRecoverSubscriber);
        }

        let res = broadcast_locked.subscribe_from(recover.next_seq()).await;
        res.map_err(StateSubscribeError::SubError)
    }

    pub async fn subscribe_fresh(
        &self,
    ) -> (RecoverableState<D>, SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>) {
        let broadcast_locked = self.broadcast.lock().await;

        loop {
            let state = {
                let mut lock = self.state_handle.lock().await;
                let state_clone = lock.current().0.clone();
                lock.quiescent();
                state_clone
            };

            let sub = match broadcast_locked.subscribe_from(state.accept_seq()).await {
                Ok(v) => v,
                Err(error) => {
                    tracing::error!(?error, "failed to subscribe to state");
                    tokio::task::yield_now().await;
                    continue;
                }
            };

            break (state, sub);
        }
    }

    pub async fn update(&self, actions: impl Iterator<Item = RecoverableStateAction<D::AuthorityAction>>) {
        let mut batch = Vec::with_capacity(actions.size_hint().0.max(4));
        let mut sender = self.broadcast_sender.lock().await;

        let read_for_state = actions.map(|action| {
            batch.push(action.clone());
            HotStateAction::Action(action)
        });

        self.state.queue_updates(read_for_state);
        for action in batch {
            let _ = sender.send(action).await;
        }

        self.settle().await;
    }
}

pub struct StateHandle<D: DeterministicState> {
    handle: HotReadHandle<HotReadRecoverable<D>>,
}

impl<D: DeterministicState> StateHandle<D> {
    pub fn current(&mut self) -> &RecoverableState<D> {
        &self.handle.current().0
    }

    pub fn recover_details(&mut self) -> RecoverableStateDetails {
        self.read_with(|v| v.details().clone())
    }

    pub fn read_with<R, F: FnOnce(&RecoverableState<D>) -> R>(&mut self, handle: F) -> R {
        let state = self.current();
        let result = handle(state);
        self.quiescent();
        result
    }

    pub fn quiescent(&mut self) {
        self.handle.quiescent();
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum StateSubscribeError {
    CannotRecoverSubscriber,
    SubError(SubscribeError),
}

#[derive(Clone)]
struct HotReadRecoverable<D: DeterministicState>(RecoverableState<D>);

#[derive(Clone)]
enum HotStateAction<D: DeterministicState> {
    Action(RecoverableStateAction<D::AuthorityAction>),
    Reset(RecoverableState<D>),
}

impl<D: DeterministicState + Clone> HotReadState for HotReadRecoverable<D> {
    type Action = HotStateAction<D>;

    fn apply_update(&mut self, update: &Self::Action) {
        match update {
            HotStateAction::Action(a) => self.0.update(a),
            HotStateAction::Reset(new_state) => {
                self.0 = new_state.clone();
            }
        }
    }
}
