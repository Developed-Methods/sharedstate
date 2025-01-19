use std::time::Duration;

use sequenced_broadcast::{SequencedBroadcast, SequencedBroadcastSettings, SequencedReceiver, SequencedSender};
use tokio::sync::{mpsc::{channel, Receiver, Sender}, oneshot};
use tracing::Instrument;

use crate::{state::{DeterministicState, SharedState}, utils::{PanicHelper, TimeoutPanicHelper}};

use super::state_updater::{UpdateInternalsAction, StateUpdater, StateUpdaterReset};

pub struct StateMaintainer<D: DeterministicState> {
    req_tx: Sender<Req<D>>,
    shared_state: SharedState<D>,
}

impl<D: DeterministicState> Clone for StateMaintainer<D> {
    fn clone(&self) -> Self {
        StateMaintainer {
            req_tx: self.req_tx.clone(),
            shared_state: self.shared_state.clone(),
        }
    }
}

struct StateMaintainerWorker<D: DeterministicState> {
    sequenced: SequencedBroadcast<D::Action>,
    broadcast_settings: SequencedBroadcastSettings,
    updater: StateUpdater<D>,
    state: SharedState<D>,
    req_rx: Receiver<Req<D>>,
}

enum Req<D: DeterministicState> {
    Reset(D, oneshot::Sender<SequencedSender<D::Action>>),
    Replace(oneshot::Sender<StateMaintainerReplaceState<D>>),
    Fresh(oneshot::Sender<(SequencedReceiver<D::Action>, D)>),
    Sub(u64, oneshot::Sender<Option<SequencedReceiver<D::Action>>>),
    NewSource(oneshot::Sender<SequencedSender<D::Action>>),
}

pub struct StateMaintainerReplaceState<D: DeterministicState> {
    replace: UpdateInternalsAction<D>,
}

impl<D: DeterministicState> StateMaintainerReplaceState<D> {
    pub fn state(&self) -> &D {
        self.replace.state()
    }

    pub fn replace_state(&mut self, state: D) -> Result<D, D> {
        self.replace.replace_state(state)
    }

    pub fn send(self) {
        self.replace.send();
    }
}

impl<D: DeterministicState> StateMaintainer<D> where D::Action: Clone {
    pub async fn new(state: D, settings: SequencedBroadcastSettings) -> (Self, SequencedSender<D::Action>) {
        let (broadcast, sender) = SequencedBroadcast::new(state.sequence(), settings.clone());

        let sequenced = broadcast.add_client(state.sequence(), false)
            .timeout(Duration::from_millis(100), "add initial client").await
            .panic("failed to add initial client");

        let (state, updater) = SharedState::new(state);
        let (req_tx, req_rx) = channel(32);

        tokio::spawn(StateMaintainerWorker {
            sequenced: broadcast,
            broadcast_settings: settings,
            updater: StateUpdater::new(sequenced, updater),
            state: state.clone(),
            req_rx,
        }.start().instrument(tracing::Span::current()));

        (
            StateMaintainer {
                req_tx,
                shared_state: state,
            },
            sender,
        )
    }

    pub fn shared_state(&self) -> SharedState<D> {
        self.shared_state.clone()
    }

    pub async fn reset(&self, state: D) -> SequencedSender<D::Action> {
        let (tx, rx) = oneshot::channel();
        self.req_tx.send(Req::Reset(state, tx)).await.panic("worker shutdown");
        rx.await.panic("response sender dropped")
    }

    pub async fn replace(&self) -> StateMaintainerReplaceState<D> {
        let (tx, rx) = oneshot::channel();
        self.req_tx.send(Req::Replace(tx)).await.panic("worker shutdown");
        rx.await.panic("response sender dropped")
    }

    pub async fn fresh_subscribe(&self) -> (SequencedReceiver<D::Action>, D) {
        let (tx, rx) = oneshot::channel();
        self.req_tx.send(Req::Fresh(tx)).await.panic("worker shutdown");
        rx.await.panic("response sender dropped")
    }

    pub async fn subscribe(&self, sequence: u64) -> Option<SequencedReceiver<D::Action>> {
        let (tx, rx) = oneshot::channel();
        self.req_tx.send(Req::Sub(sequence, tx)).await.panic("worker shutdown");
        rx.await.panic("response sender dropped")
    }

    pub async fn new_source(&self) -> SequencedSender<D::Action> {
        let (tx, rx) = oneshot::channel();
        self.req_tx.send(Req::NewSource(tx)).await.panic("worker shutdown");
        rx.await.panic("response sender dropped")
    }
}

impl<D: DeterministicState> StateMaintainerWorker<D> where D::Action: Clone {
    async fn start(mut self) {
        while let Some(req) = self.req_rx.recv().await {
            match req {
                Req::Reset(state, sender_rx) => {
                    /* kills all subscribers */
                    self.sequenced.shutdown();

                    let (broadcast, sender) = SequencedBroadcast::new(state.sequence(), self.broadcast_settings.clone());

                    self.sequenced = broadcast;

                    let client = self.sequenced.add_client(state.sequence(), false)
                        .await.expect("failed to add client to new broadcast");

                    self.updater.update_internals(true)
                        .timeout(Duration::from_millis(500), "timeout waiting for updater to replace state").await
                        .reset(StateUpdaterReset::new(state, client).panic("invalid sequences"));

                    let _ = sender_rx.send(sender);
                }
                Req::Replace(resp) => {
                    let replace = self.updater.update_internals(false)
                        .timeout(Duration::from_millis(100), "timeout waiting for replace state").await;
                    let _ = resp.send(StateMaintainerReplaceState { replace });
                }
                Req::Sub(seq, resp) => {
                    let client = self.sequenced.add_client(seq, true)
                        .timeout(Duration::from_millis(100), "failed to add new client").await;
                    let _ = resp.send(client);
                }
                Req::Fresh(resp) => {
                    /* not marked send to avoid high likelyhood for dead locks */
                    let state = UnsafeSendWrap(self.state.read());

                    let client = self.sequenced.add_client(state.0.sequence(), true)
                        .timeout(Duration::from_millis(100), "add client on fresh state").await
                        .panic("cannot add client on sequence with state locked");

                    let _ = resp.send((client, state.0.clone()));
                }
                Req::NewSource(resp) => {
                    let sender = self.sequenced.replace_sender(0)
                        .timeout(Duration::from_millis(100), "timeout replace sequenced sender").await
                        .panic("failed to replace sender with 0 seq");

                    let _ = resp.send(sender);
                }
            }
        }
    }
}

struct UnsafeSendWrap<T>(T);

unsafe impl<T> Send for UnsafeSendWrap<T> {}

#[cfg(test)]
mod test {
    use sequenced_broadcast::SequencedSenderError;

    use super::*;
    use crate::testing::state_tests::*;

    #[tokio::test]
    async fn state_manager_test() {
        let (maintainer, mut sender) = StateMaintainer::new(
            TestState::default(),
            SequencedBroadcastSettings::default()
        ).await;

        let state = maintainer.shared_state();

        sender.send(TestStateAction::Add { slot: 1, value: 3 }).await.unwrap();
        sender.send(TestStateAction::Add { slot: 1, value: 5 }).await.unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(state.read().sequence(), 2);
        assert_eq!(state.read().numbers[1], 8);

        let mut sender = maintainer.new_source().await;
        sender.send(TestStateAction::Add { slot: 1, value: 1 }).await.unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(state.read().sequence(), 3);
        assert_eq!(state.read().numbers[1], 9);

        let mut sub = maintainer.subscribe(1).await.unwrap();
        assert_eq!(sub.recv().timeout(Duration::from_millis(10), "recv").await.unwrap(), (1, TestStateAction::Add { slot: 1, value: 5 }));
        assert_eq!(sub.recv().timeout(Duration::from_millis(10), "recv").await.unwrap(), (2, TestStateAction::Add { slot: 1, value: 1 }));

        let mut new_sender = maintainer.reset(TestState {
            sequence: 32,
            numbers: [1, 2, 3, 4, 5, 6],
        }).await;

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(sender.send(TestStateAction::Add { slot: 1, value: 1 }).await, Err(SequencedSenderError::ChannelClosed(TestStateAction::Add { slot: 1, value: 1 })));

        assert_eq!(sub.recv().timeout(Duration::from_millis(10), "recv").await, None);
        assert_eq!(state.read().sequence(), 32);
        assert_eq!(state.read().numbers[1], 2);

        new_sender.safe_send(32, TestStateAction::Add { slot: 1, value: 1 }).await.unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(state.read().sequence(), 33);
        assert_eq!(state.read().numbers[1], 3);

        let (mut updates, state) = maintainer.fresh_subscribe().await;
        assert_eq!(state.sequence(), 33);

        new_sender.safe_send(33, TestStateAction::Add { slot: 0, value: 100 }).await.unwrap();
        assert_eq!(updates.recv().timeout(Duration::from_millis(10), "").await, Some((33, TestStateAction::Add { slot: 0, value: 100 })));
    }
}

