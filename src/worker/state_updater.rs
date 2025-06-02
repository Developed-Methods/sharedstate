use std::{sync::{atomic::{AtomicU64, Ordering}, Arc, LazyLock}, time::Instant};

use sequenced_broadcast::{SequencedReceiver, SequencedSender, SequencedSenderError};
use tokio::sync::{mpsc::{channel, error::TryRecvError, Receiver, Sender}, oneshot};

use crate::{state::{DeterministicState, FollowUpdater, LeadUpdater, StatePtr}, utils::PanicHelper};

pub struct StateUpdater<D: DeterministicState> {
    replace_req_tx: Sender<ReplaceRequest<D>>,
    worker_loop: Arc<AtomicU64>,
}

struct StateUpdaterWorker<D: DeterministicState> {
    internals: StateUpdaterInternals<D>,
    next_update_action: Option<ReplaceRequest<D>>,
    next_update_action_rx: Receiver<ReplaceRequest<D>>,
    worker_loop: Arc<AtomicU64>,
}

enum StateUpdaterInternals<D: DeterministicState> {
    Leader {
        updater: LeadUpdater<D>,
        rx: Receiver<D::Action>,
        next: Option<D::Action>,

        tx: SequencedSender<D::AuthorityAction>,
    },
    Follower {
        updater: FollowUpdater<D>,
        rx: SequencedReceiver<D::AuthorityAction>,
        next: Option<(u64, D::AuthorityAction)>,

        tx: SequencedSender<D::AuthorityAction>,
    },
}

struct ReplaceRequest<D: DeterministicState> {
    wait_for_rx_closed: bool,
    response: oneshot::Sender<UpdateInternalsAction<D>>,
}

pub struct UpdateInternalsAction<D: DeterministicState> {
    inner: Option<ReplaceStateActionInner<D>>,
}

struct ReplaceStateActionInner<D: DeterministicState> {
    internals: StateUpdaterInternals<D>,
    provide: oneshot::Sender<StateUpdaterInternals<D>>,
}

impl<D: DeterministicState> StateUpdater<D> where D::AuthorityAction: Clone {
    pub fn leader(rx: Receiver<D::Action>, updater: LeadUpdater<D>) -> (Self, SequencedReceiver<D::AuthorityAction>) {
        let (authority_tx, authority_rx) = channel::<(u64, D::AuthorityAction)>(1024);
        let authority_tx = SequencedSender::new(updater.next_sequence(), authority_tx);
        let authority_rx = SequencedReceiver::new(updater.next_sequence(), authority_rx);

        let (replace_tx, replace_rx) = channel::<ReplaceRequest<D>>(1);
        let worker_loop = Arc::new(AtomicU64::new(0));

        let internals = StateUpdaterInternals::Leader { updater, rx, tx: authority_tx, next: None };
        tokio::spawn(StateUpdaterWorker {
            internals,
            next_update_action: None,
            next_update_action_rx: replace_rx,
            worker_loop: worker_loop.clone(),
        }.start());

        (
            StateUpdater {
                replace_req_tx: replace_tx,
                worker_loop,
            },
            authority_rx,
        )
    }

    pub fn follower(rx: SequencedReceiver<D::AuthorityAction>, updater: FollowUpdater<D>) -> (Self, SequencedReceiver<D::AuthorityAction>) {
        let (authority_tx, authority_rx) = channel::<(u64, D::AuthorityAction)>(1024);
        let authority_tx = SequencedSender::new(updater.next_sequence(), authority_tx);
        let authority_rx = SequencedReceiver::new(updater.next_sequence(), authority_rx);

        let internals = StateUpdaterInternals::Follower { updater, rx, tx: authority_tx, next: None };
        let (replace_tx, replace_rx) = channel::<ReplaceRequest<D>>(1);
        let worker_loop = Arc::new(AtomicU64::new(0));

        tokio::spawn(StateUpdaterWorker {
            internals,
            next_update_action: None,
            next_update_action_rx: replace_rx,
            worker_loop: worker_loop.clone(),
        }.start());

        (
            StateUpdater {
                replace_req_tx: replace_tx,
                worker_loop,
            },
            authority_rx
        )
    }

    pub async fn update_internals(&mut self, wait_for_close: bool) -> UpdateInternalsAction<D> {
        let (tx, rx) = oneshot::channel();

        self.replace_req_tx.send(ReplaceRequest {
            wait_for_rx_closed: wait_for_close,
            response: tx,
        }).await.panic("worker closed");

        let mut internals = rx.await.panic("worker closed");

        let inner = internals.inner.as_mut().unwrap();
        match &mut inner.internals {
            StateUpdaterInternals::Leader { updater, .. } => updater.flush(),
            StateUpdaterInternals::Follower { updater, .. } => updater.flush(),
        }

        internals
    }

    pub fn worker_loop(&self) -> u64 {
        self.worker_loop.load(Ordering::Acquire)
    }
}

pub struct StateAndReceiver<D: DeterministicState> {
    state: D,
    receiver: SequencedReceiver<D::AuthorityAction>,
}

impl<D: DeterministicState> StateAndReceiver<D> {
    pub fn create(state: D, receiver: SequencedReceiver<D::AuthorityAction>) -> Result<Self, (D, SequencedReceiver<D::AuthorityAction>)> {
        if state.sequence() != receiver.next_seq() {
            return Err((state, receiver));
        }
        Ok(StateAndReceiver {
            state,
            receiver
        })
    }
}


impl<D: DeterministicState> UpdateInternalsAction<D> {
    pub fn next_sequence(&self) -> u64 {
        match &self.inner.as_ref().unwrap().internals {
            StateUpdaterInternals::Follower { updater, .. } => updater.next_sequence(),
            StateUpdaterInternals::Leader { updater, .. } => updater.next_sequence(),
        }
    }

    pub fn use_state<R, F: Fn(&D) -> R>(&self, visit: F) -> R {
        let inner = self.inner.as_ref().unwrap();

        match &inner.internals {
            StateUpdaterInternals::Leader { updater, .. } => visit(updater.state()),
            StateUpdaterInternals::Follower { updater, .. } => {
                let lock = updater.read_state();
                visit(&*lock)
            }
        }
    }

    pub fn reset_follow(&mut self, state_rx: StateAndReceiver<D>) -> SequencedReceiver<D::AuthorityAction> {
        let (tx, rx) = channel(1024);
        let tx = SequencedSender::new(state_rx.state.sequence(), tx);
        let rx = SequencedReceiver::new(state_rx.state.sequence(), rx);

        let mut inner = self.inner.take().panic("missing inner");
        inner.internals = match inner.internals {
            StateUpdaterInternals::Leader { updater, .. } => {
                let mut updater = updater.into_follow();
                updater.reset(state_rx.state);

                StateUpdaterInternals::Follower {
                    updater,
                    rx: state_rx.receiver,
                    tx,
                    next: None
                }
            }
            StateUpdaterInternals::Follower { mut updater, .. } => {
                updater.reset(state_rx.state);

                StateUpdaterInternals::Follower {
                    updater,
                    rx: state_rx.receiver,
                    tx,
                    next: None,
                }
            }
        };

        self.inner = Some(inner);
        rx
    }

    pub fn setup_follow<R, F: FnOnce(&mut StatePtr<D>) -> R>(&mut self, action_rx: SequencedReceiver<D::AuthorityAction>, update: F) -> Result<R, u64> {
        if self.next_sequence() != action_rx.next_seq() {
            tracing::error!("setup_follow invalid sequence, expected: {} but got: {}", self.next_sequence(), action_rx.next_seq());
            return Err(self.next_sequence());
        }

        let mut inner = self.inner.take().panic("missing inner");

        let (mut updater, tx) = match inner.internals {
            StateUpdaterInternals::Leader { updater, tx, .. } => (updater.into_follow(), tx),
            StateUpdaterInternals::Follower { updater, tx, .. } => (updater, tx),
        };

        let result = updater.update_state(update);

        inner.internals = StateUpdaterInternals::Follower {
            updater,
            rx: action_rx,
            tx,
            next: None,
        };

        self.inner = Some(inner);
        Ok(result)
    }

    pub fn become_leader<F: FnOnce(&mut StatePtr<D>) -> bool>(&mut self, update: F, action_rx: Receiver<D::Action>) -> Option<SequencedReceiver<D::AuthorityAction>> {
        let mut inner = self.inner.take().panic("missing inner");
        let mut new_rx = None;

        let (updater, tx) = match inner.internals {
            StateUpdaterInternals::Leader { updater, tx, .. } => (updater, tx),
            StateUpdaterInternals::Follower { updater, tx, .. } => {
                let mut updater = updater.into_lead();

                let seq = updater.next_sequence();
                let should_update_feed = updater.update_state(update);

                if should_update_feed || seq != updater.next_sequence() {
                    let (new_tx, rx) = channel(1024);
                    let seq = updater.next_sequence();
                    let new_tx = SequencedSender::new(seq, new_tx);
                    new_rx = Some(SequencedReceiver::new(seq, rx));

                    (updater, new_tx)
                } else {
                    (updater, tx)
                }
            }
        };

        inner.internals = StateUpdaterInternals::Leader {
            updater,
            rx: action_rx,
            tx,
            next: None,
        };

        self.inner = Some(inner);
        new_rx
    }

    pub fn apply(self) {
    }
}

impl<D: DeterministicState> Drop for UpdateInternalsAction<D> {
    fn drop(&mut self) {
        let inner = self.inner.take().panic("inner already taken");
        inner.provide.send(
            inner.internals
        ).panic("worker shutdown");
    }
}

static WORKER_ID: LazyLock<Arc<AtomicU64>> = LazyLock::new(|| Arc::new(AtomicU64::new(1)));

impl<D: DeterministicState> StateUpdaterWorker<D> where D::AuthorityAction: Clone {
    async fn start(self) {
        let id = WORKER_ID.fetch_add(1, Ordering::SeqCst);
        tracing::info!(id, "{}|StateUpdaterWorker Started", id);
        let start = Instant::now();

        self._start(id).await;
        let elapsed = start.elapsed();
        tracing::info!(id, ?elapsed, "{}|StateUpdaterWorker Stopped", id);
    }

    async fn _start(mut self, id: u64) {
        let mut internals_dead = false;

        'main_loop: loop {
            self.worker_loop.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;

            if self.next_update_action.is_none() {
                if internals_dead {
                    internals_dead = false;

                    self.next_update_action = self.next_update_action_rx.recv().await;
                    if self.next_update_action.is_none() {
                        tracing::info!("{}|RX disconnected and no more updates available, closing worker", id);
                        break;
                    }
                } else {
                    self.next_update_action = match self.next_update_action_rx.try_recv() {
                        Ok(v) => Some(v),
                        Err(TryRecvError::Empty) => None,
                        Err(TryRecvError::Disconnected) => {
                            tracing::info!("{}|RX disconnected and no more updates available, closing worker", id);
                            break;
                        }
                    };
                }
            }

            while let Some(replace) = self.next_update_action.take() {
                if replace.wait_for_rx_closed && !internals_dead {
                    self.next_update_action = Some(replace);
                    break;
                }

                let (tx, rx) = oneshot::channel();

                replace.response
                    .send(UpdateInternalsAction {
                        inner: Some(ReplaceStateActionInner {
                            internals: self.internals,
                            provide: tx,
                        })
                    })
                    .panic("failed to send update actions to caller");

                self.internals = rx.await
                    .panic("update did not return internals");

                self.next_update_action = self.next_update_action_rx.try_recv().ok();
            }

            let mut blocked = false;

            match &mut self.internals {
                StateUpdaterInternals::Leader { updater, rx, tx, next } => {
                    for _ in 0..1024 {
                        let action = match next.take() {
                            Some(action) => action,
                            None => {
                                match rx.try_recv() {
                                    Ok(action) => action,
                                    Err(TryRecvError::Disconnected) => {
                                        internals_dead = true;
                                        continue 'main_loop;
                                    }
                                    Err(TryRecvError::Empty) => {
                                        blocked = true;
                                        break;
                                    }
                                }
                            }
                        };

                        let (seq, authority) = updater.queue(action);
                        match tx.safe_send(seq, authority.clone()).await {
                            Ok(_) => {}
                            Err(SequencedSenderError::InvalidSequence(expected, _)) => {
                                panic!("sending invalid sequence, expected: {} but sent: {}", expected, seq);
                            }
                            Err(SequencedSenderError::ChannelClosed(_)) => {
                                tracing::warn!("{}|leader authority queue closed", id);
                                internals_dead = true;
                                continue 'main_loop;
                            }
                        }
                    }

                    updater.update();

                    if !blocked {
                        continue 'main_loop;
                    }

                    tokio::select! {
                        action = self.next_update_action_rx.recv() => {
                            /* note: if none (closed), top of loop will kill worker */
                            self.next_update_action = action;
                        }
                        action = rx.recv() => {
                            *next = action;
                            if next.is_none() {
                                internals_dead = true;
                            }
                        }
                    }
                }
                StateUpdaterInternals::Follower { updater, rx, tx, next } => {
                    for _ in 0..1024 {
                        let (seq, action) = match next.take() {
                            Some(action) => action,
                            None => {
                                match rx.try_recv() {
                                    Ok(action) => action,
                                    Err(TryRecvError::Disconnected) => {
                                        internals_dead = true;
                                        continue 'main_loop;
                                    }
                                    Err(TryRecvError::Empty) => {
                                        blocked = true;
                                        break;
                                    }
                                }
                            }
                        };

                        if !updater.queue(seq, action.clone()) {
                            panic!("{}|invalid sequence, expected: {} but got: {}", id, updater.next_sequence(), seq);
                        }

                        match tx.safe_send(seq, action).await {
                            Ok(_) => {}
                            Err(SequencedSenderError::InvalidSequence(expected, _)) => {
                                panic!("{}|sending invalid sequence, expected: {} but sent: {}", id, expected, seq);
                            }
                            Err(SequencedSenderError::ChannelClosed(_)) => {
                                tracing::warn!("{}|leader authority queue closed", id);
                                internals_dead = true;
                                continue 'main_loop;
                            }
                        }
                    }

                    updater.update();

                    if !blocked {
                        continue 'main_loop;
                    }

                    tokio::select! {
                        action = self.next_update_action_rx.recv() => {
                            /* note: if none (closed), top of loop will kill worker */
                            self.next_update_action = action;
                        }
                        action = rx.recv() => {
                            *next = action;
                            if next.is_none() {
                                internals_dead = true;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use assert_matches::assert_matches;

    use crate::{state::SharedState, testing::state_tests::{TestState, TestStateAction}};

    use super::*;

    #[tokio::test]
    async fn state_updater_test() {
        let (shared, updater) = SharedState::new(TestState::default());

        let (tx, rx) = channel(1024);
        let (mut updater, mut rx) = StateUpdater::leader(rx, updater.into_lead());

        tx.send(TestStateAction::Add { slot: 0, value: 33 }).await.unwrap();
        tx.send(TestStateAction::Add { slot: 1, value: 22 }).await.unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        let seq = {
            let lock = shared.read();
            let state = &*lock;

            assert_eq!(state.numbers[0], 33);
            assert_eq!(state.numbers[1], 22);
            state.sequence()
        };

        assert_matches!(rx.recv().await.unwrap(), (0, (_, TestStateAction::Add { slot: 0, value: 33 })));
        assert_matches!(rx.recv().await.unwrap(), (1, (_, TestStateAction::Add { slot: 1, value: 22 })));
        assert_eq!(seq, 2);

        /* go to follower */
        {
            let mut update = updater.update_internals(false).await;

            let (tx, rx) = channel(1024);
            let mut tx = SequencedSender::new(seq, tx);
            let rx = SequencedReceiver::new(seq, rx);

            update.setup_follow(rx, |_| ()).unwrap();
            update.apply();

            tx.safe_send(2, (100, TestStateAction::Add { slot: 0, value: 10 })).await.unwrap();
            tx.safe_send(3, (100, TestStateAction::Add { slot: 1, value: 20 })).await.unwrap();

            tokio::time::sleep(Duration::from_millis(10)).await;

            let seq = {
                let lock = shared.read();
                let state = &*lock;

                assert_eq!(state.numbers[0], 43);
                assert_eq!(state.numbers[1], 42);
                state.sequence()
            };

            assert_eq!(seq, 4);
        }

        /* go to leader */
        let (tx, mut rx) = {
            let mut update = updater.update_internals(false).await;
            let (tx, rx) = channel(1024);

            let rx = update.become_leader(|state| {
                state.sequence = 1;
                state.numbers[0] = 100;
                state.numbers[1] = 200;
                true
            }, rx).unwrap();

            update.apply();

            (tx, rx)
        };

        tokio::time::sleep(Duration::from_millis(10)).await;

        {
            let lock = shared.read();
            let state = &*lock;

            assert_eq!(state.numbers[0], 100);
            assert_eq!(state.numbers[1], 200);
            assert_eq!(1, state.sequence());
        }

        tx.send(TestStateAction::Add { slot: 0, value: 99 }).await.unwrap();
        assert_matches!(rx.recv().await.unwrap(), (1, (_, TestStateAction::Add { slot: 0, value: 99 })));

        {
            let lock = shared.read();
            let state = &*lock;

            assert_eq!(state.numbers[0], 199);
            assert_eq!(state.numbers[1], 200);
            assert_eq!(2, state.sequence());
        }
    }
}
