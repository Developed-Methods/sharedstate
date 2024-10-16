use sequenced_broadcast::SequencedReceiver;
use tokio::sync::{mpsc::{channel, error::TryRecvError, Receiver, Sender}, oneshot};

use crate::{state::{DeterministicState, SharedStateUpdater}, utils::PanicHelper};

pub struct StateUpdater<D: DeterministicState> {
    replace_req_tx: Sender<ReplaceRequest<D>>,
}

struct ReplaceRequest<D: DeterministicState> {
    wait_for_rx_closed: bool,
    response: oneshot::Sender<ReplaceState<D>>,
}

impl<D: DeterministicState> StateUpdater<D> {
    pub fn fresh(updater: SharedStateUpdater<D>) -> Self {
        let (_, rx) = channel(1);
        Self::new(SequencedReceiver::new(updater.next_queued_sequence(), rx), updater)
    }

    pub fn new(rx: SequencedReceiver<D::Action>, updater: SharedStateUpdater<D>) -> Self {
        assert_eq!(rx.next_seq(), updater.next_queued_sequence());

        let (replace_req_tx, replace_req_rx) = channel(1);

        tokio::spawn(StateUpdaterTask {
            rx,
            updater,
            replace_req_rx,
            next_replace: None,
        }.start());

        StateUpdater {
            replace_req_tx,
        }
    }

    pub async fn replace_state(&mut self, wait_for_close: bool) -> ReplaceState<D> {
        let (tx, rx) = oneshot::channel();

        self.replace_req_tx.send(ReplaceRequest {
            wait_for_rx_closed: wait_for_close,
            response: tx,
        }).await.panic("worker closed");

        rx.await.panic("worker closed")
    }
}

pub struct ReplaceState<D: DeterministicState> {
    inner: Option<ReplaceInner<D>>,
}

pub struct StateUpdaterReset<D: DeterministicState> {
    state: D,
    receiver: SequencedReceiver<D::Action>,
}

impl<D: DeterministicState> StateUpdaterReset<D> {
    pub fn new(state: D, receiver: SequencedReceiver<D::Action>) -> Result<Self, (D, SequencedReceiver<D::Action>)> {
        if state.sequence() != receiver.next_seq() {
            return Err((state, receiver));
        }

        Ok(StateUpdaterReset {
            state,
            receiver
        })
    }
}

impl<D: DeterministicState> ReplaceState<D> {
    pub fn sequence(&self) -> u64 {
        self.state().sequence()
    }

    pub fn state(&self) -> &D {
        let inner = self.inner.as_ref().unwrap();
        &inner.state
    }

    pub fn reset(mut self, reset: StateUpdaterReset<D>) -> (D, SequencedReceiver<D::Action>) {
        let inner = self.inner.as_mut().unwrap();
        let old_state = std::mem::replace(&mut inner.state, reset.state);
        let old_rx = std::mem::replace(&mut inner.receiver, reset.receiver);
        (old_state, old_rx)
    }

    pub fn replace_state(&mut self, state: D) -> Result<D, D> {
        let inner = self.inner.as_mut().unwrap();
        if state.sequence() != inner.state.sequence() {
            return Err(state);
        }
        Ok(std::mem::replace(&mut inner.state, state))
    }

    pub fn replace_receiver(&mut self, rx: SequencedReceiver<D::Action>) -> Result<SequencedReceiver<D::Action>, SequencedReceiver<D::Action>> {
        let inner = self.inner.as_mut().unwrap();
        if rx.next_seq() != inner.receiver.next_seq() {
            return Err(rx);
        }
        Ok(std::mem::replace(&mut inner.receiver, rx))
    }

    pub fn send(self) {
        /* send handled by drop */
    }
}

struct ReplaceInner<D: DeterministicState> {
    state: D,
    receiver: SequencedReceiver<D::Action>,
    provide: oneshot::Sender<(D, SequencedReceiver<D::Action>)>,
}

impl<D: DeterministicState> Drop for ReplaceState<D> {
    fn drop(&mut self) {
        let inner = self.inner.take().unwrap();

        inner.provide.send((
            inner.state,
            inner.receiver,
        )).panic("worker shutdown");
    }
}

pub struct StateUpdaterTask<D: DeterministicState> {
    rx: SequencedReceiver<D::Action>,
    updater: SharedStateUpdater<D>,
    replace_req_rx: Receiver<ReplaceRequest<D>>,
    next_replace: Option<ReplaceRequest<D>>,
}

impl<D: DeterministicState> StateUpdaterTask<D> {
    pub async fn start(mut self) {
        loop {
            while let Some(replace) = self.next_replace.take() {
                if replace.wait_for_rx_closed && !self.rx.is_closed() {
                    self.next_replace = Some(replace);
                    break;
                }

                self.updater.flush_queue();
                let state = self.updater.read().clone();

                let (tx, rx) = oneshot::channel();

                replace.response.send(ReplaceState {
                    inner: Some(ReplaceInner {
                        state,
                        receiver: self.rx,
                        provide: tx
                    })
                }).panic("replace request caller closed response channel");

                let (
                    new_state,
                    receiver,
                ) = rx.await.panic("replace response not provided");

                assert_eq!(new_state.sequence(), receiver.next_seq());

                self.updater.reset(new_state);
                self.rx = receiver;

                match self.replace_req_rx.try_recv() {
                    Ok(item) => {
                        self.next_replace = Some(item);
                    }
                    Err(TryRecvError::Disconnected) => {
                        tracing::info!("StateUpdater dropped, closing worker");
                        return;
                    }
                    Err(TryRecvError::Empty) => {
                        break;
                    }
                }
            }

            'next_rx: loop {
                tokio::task::yield_now().await;

                if self.updater.update_ready() {
                    self.updater.update();
                }

                let next = if self.next_replace.is_none() {
                    tokio::select! {
                        update = self.replace_req_rx.recv() => {
                            let Some(update) = update else {
                                tracing::info!("StateUpdater dropped, closing worker");
                                return;
                            };

                            self.next_replace = Some(update);
                            break;
                        },
                        next = self.rx.recv() => next,
                    }
                } else {
                    self.rx.recv().await
                };

                let Some((seq, action)) = next else {
                    break
                };

                self.updater.queue_sequenced(seq, action)
                    .panic("invalid sequence provided to state");

                for _ in 0..1024 {
                    match self.rx.try_recv() {
                        Ok((seq, action)) => {
                            if self.updater.queue_sequenced(seq, action).is_err() {
                                panic!("invalid sequence provided to state");
                            }
                        }
                        Err(TryRecvError::Empty) => continue 'next_rx,
                        Err(TryRecvError::Disconnected) => break 'next_rx,
                    }
                }

                self.updater.update();
                if self.updater.update_ready() {
                    self.updater.update();
                }
            }
        }
    }
}

