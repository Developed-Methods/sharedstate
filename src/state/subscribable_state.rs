use std::time::Duration;

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
    ///
    /// A reader handle that never goes quiescent blocks publication
    /// indefinitely; after a burst of yields this backs off to sleeping (so a
    /// stuck reader degrades update latency instead of busy-spinning a core)
    /// and warns periodically so the stall is diagnosable.
    async fn settle(&self) {
        const YIELD_LIMIT: u32 = 1024;
        const RETRY_DELAY: Duration = Duration::from_millis(1);
        const WARN_INTERVAL: Duration = Duration::from_secs(1);

        let started = tokio::time::Instant::now();
        let mut last_warned: Option<tokio::time::Instant> = None;
        let mut yields = 0u32;

        loop {
            if self.state.copy_applied_seq(self.state.active_index()) == self.state.latest_seq() {
                return;
            }
            let maintenance = self.state.maintain();

            if yields < YIELD_LIMIT {
                yields += 1;
                tokio::task::yield_now().await;
                continue;
            }

            if yields == YIELD_LIMIT {
                last_warned = Some(tokio::time::Instant::now());
            } else if last_warned.is_none_or(|at| WARN_INTERVAL <= at.elapsed()) {
                tracing::warn!(
                    blocked_by_workers = maintenance.blocked_by_workers,
                    elapsed = ?started.elapsed(),
                    "state updates are not settling; a state handle may be missing a quiescent() call"
                );

                last_warned = Some(tokio::time::Instant::now());
            }

            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    pub async fn reset(&self, new_state: RecoverableState<D>) {
        let mut broadcast_locked = self.broadcast.lock().await;
        let mut sender_locked = self.broadcast_sender.lock().await;

        let new_recover_details = new_state.details().clone();
        tracing::info!(next_seq = new_recover_details.next_seq(), "resetting subscribable state from fresh snapshot");

        self.state.queue_update(HotStateAction::Reset(new_state));
        self.settle().await;

        let (broadcast, broadcast_sender) =
            SequencedBroadcast::new(new_recover_details.next_seq(), self.broadcast_settings.clone()).unwrap();

        *broadcast_locked = broadcast;
        let mut old_sender = std::mem::replace(&mut *sender_locked, broadcast_sender);
        old_sender.close();
        tracing::info!(next_seq = new_recover_details.next_seq(), "subscribable state reset complete");
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
            tracing::info!(
                leader_next_seq = leader_details.next_seq(),
                subscriber_next_seq = recover.next_seq(),
                "state subscriber cannot recover incrementally"
            );
            return Err(StateSubscribeError::CannotRecoverSubscriber);
        }

        let res = broadcast_locked.subscribe_from(recover.next_seq()).await;
        match res {
            Ok(sub) => {
                tracing::info!(
                    subscriber_next_seq = recover.next_seq(),
                    "state subscriber registered for incremental updates"
                );
                Ok(sub)
            }
            Err(error) => {
                tracing::info!(
                    subscriber_next_seq = recover.next_seq(),
                    ?error,
                    "state subscriber failed to register for incremental updates"
                );
                Err(StateSubscribeError::SubError(error))
            }
        }
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
                Ok(v) => {
                    tracing::info!(
                        next_seq = state.accept_seq(),
                        "state subscriber registered for fresh snapshot updates"
                    );
                    v
                }
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

#[cfg(test)]
mod tests {
    use std::{iter, sync::Arc};

    use super::*;
    use crate::state::deterministic_state::DeterministicState;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Counter(u64);

    impl DeterministicState for Counter {
        type Action = u64;
        type AuthorityAction = u64;

        fn accept_seq(&self) -> u64 {
            self.0
        }

        fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
            action
        }

        fn update(&mut self, _action: &Self::AuthorityAction) {
            self.0 += 1;
        }
    }

    fn new_state() -> Arc<SubscribableState<Counter>> {
        Arc::new(
            SubscribableState::new(RecoverableState::new(1, Counter(0)), SequencedBroadcastSettings::default())
                .unwrap(),
        )
    }

    fn action() -> RecoverableStateAction<u64> {
        RecoverableStateAction::StateAction { action: 7 }
    }

    /// A handle that read the state and never went quiescent blocks hot-read
    /// publication: updates must wait (without busy-spinning forever being
    /// the only observable behavior) and resume once the reader lets go.
    #[tokio::test(start_paused = true)]
    async fn pinned_read_handle_defers_updates_until_quiescent() {
        let state = new_state();
        let mut handle = state.create_handle();

        /* pin the currently published copy mid-read */
        let _ = handle.current();

        let updates = {
            let state = state.clone();
            tokio::spawn(async move {
                state.update(iter::once(action())).await;
                state.update(iter::once(action())).await;
            })
        };

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!updates.is_finished(), "updates must not settle while a read handle is pinned");

        handle.quiescent();
        tokio::time::timeout(std::time::Duration::from_secs(5), updates)
            .await
            .expect("updates must settle once the reader goes quiescent")
            .unwrap();

        assert_eq!(handle.read_with(|state| state.state().0), 2);
    }
}
