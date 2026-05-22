use sequenced_broadcast::{SequencedReceiver, SequencedSender};
use tokio::sync::mpsc::channel;
use tokio_util::sync::CancellationToken;

use crate::state::{DeterministicState, FlushedUpdater, FollowUpdater};

use super::{
    task_and_cancel::TaskAndCancel,
    update_worker_loop::{
        drain_pending, process_batch, update_if_ready, UpdateWorker, UPDATE_TICK,
    },
};

pub struct FollowWorker<D: DeterministicState> {
    updater: FollowUpdater<D>,
    rx: SequencedReceiver<D::AuthorityAction>,
    tx: SequencedSender<D::AuthorityAction>,
}

pub struct SequencedRxAndUpdater<D: DeterministicState> {
    pub rx: SequencedReceiver<D::AuthorityAction>,
    pub updater: FlushedUpdater<D>,
}

pub struct ValidSequencedRxAndUpdater<D: DeterministicState>(SequencedRxAndUpdater<D>);

impl<D: DeterministicState> SequencedRxAndUpdater<D> {
    pub fn is_valid_pair(&self) -> bool {
        self.rx.next_seq() == self.updater.accept_seq()
    }

    pub fn try_into_valid(self) -> Result<ValidSequencedRxAndUpdater<D>, SequencedRxAndUpdater<D>> {
        if self.is_valid_pair() {
            Ok(ValidSequencedRxAndUpdater(self))
        } else {
            Err(self)
        }
    }
}

impl<D: DeterministicState> FollowWorker<D>
where
    D::AuthorityAction: Clone,
{
    pub fn new(
        rx_and_updater: ValidSequencedRxAndUpdater<D>,
    ) -> (Self, SequencedReceiver<D::AuthorityAction>) {
        let (ch_tx, ch_rx) = channel(2048);

        let seq = rx_and_updater.0.updater.accept_seq();
        let seq_tx = SequencedSender::new(seq, ch_tx);
        let seq_rx = SequencedReceiver::new(seq, ch_rx);

        (
            FollowWorker {
                updater: rx_and_updater.0.updater.into_follow(),
                rx: rx_and_updater.0.rx,
                tx: seq_tx,
            },
            seq_rx,
        )
    }

    pub fn spawn(mut self) -> TaskAndCancel<Self> {
        TaskAndCancel::spawn(|cancel| async move {
            self.run_till_canceled(cancel).await;
            self
        })
    }

    pub async fn run_till_canceled(&mut self, cancel: CancellationToken) {
        let mut tick = tokio::time::interval(UPDATE_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::task::yield_now().await;

            let (seq, action) = tokio::select! {
                _ = tick.tick() => {
                    update_if_ready(self);
                    continue;
                }
                _ = cancel.cancelled() => break,
                msg_opt = self.rx.recv() => {
                    let Some(msg) = msg_opt else { break };
                    msg
                }
            };

            process_batch(self, (seq, action)).await;
        }

        drain_pending(self).await;
    }

    pub fn into_flushed(self) -> FlushedUpdater<D> {
        self.updater.into_flushed()
    }
}

impl<D: DeterministicState> UpdateWorker for FollowWorker<D>
where
    D::AuthorityAction: Clone,
{
    type Item = (u64, D::AuthorityAction);
    type TryRecvError = tokio::sync::mpsc::error::TryRecvError;

    fn try_recv_update_item(&mut self) -> Result<Self::Item, Self::TryRecvError> {
        self.rx.try_recv()
    }

    fn apply_update_item<'a>(
        &'a mut self,
        (seq, action): Self::Item,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>
    where
        Self::Item: 'a,
    {
        Box::pin(async move {
            let authority = self.updater.queue(seq, action);
            self.tx.safe_send(seq, authority.clone()).await.unwrap();
        })
    }

    fn update_ready(&mut self) -> bool {
        self.updater.update_ready()
    }

    fn update(&mut self) {
        self.updater.update();
    }
}
