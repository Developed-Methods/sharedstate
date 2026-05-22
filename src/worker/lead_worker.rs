use sequenced_broadcast::{SequencedReceiver, SequencedSender};
use tokio::sync::mpsc::{channel, Receiver};
use tokio_util::sync::CancellationToken;

use crate::{
    state::{DeterministicState, FlushedUpdater, LeadUpdater},
    utils::PanicHelper,
};

use super::{
    task_and_cancel::TaskAndCancel,
    update_worker_loop::{
        drain_pending, process_batch, update_if_ready, UpdateWorker, UPDATE_TICK,
    },
};

pub struct LeadWorker<D: DeterministicState> {
    updater: LeadUpdater<D>,
    rx: Receiver<D::Action>,
    tx: SequencedSender<D::AuthorityAction>,
}

impl<D: DeterministicState> LeadWorker<D>
where
    D::AuthorityAction: Clone,
{
    pub fn spawn(mut self) -> TaskAndCancel<Self> {
        TaskAndCancel::spawn(|cancel| async move {
            self.run_until_cancelled(cancel).await;
            self
        })
    }

    pub async fn run_until_cancelled(&mut self, cancel: CancellationToken) {
        let mut tick = tokio::time::interval(UPDATE_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::task::yield_now().await;

            let action = tokio::select! {
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

            process_batch(self, action).await;
        }

        drain_pending(self).await;
    }
}

impl<D: DeterministicState> UpdateWorker for LeadWorker<D>
where
    D::AuthorityAction: Clone,
{
    type Item = D::Action;
    type TryRecvError = tokio::sync::mpsc::error::TryRecvError;

    fn try_recv_update_item(&mut self) -> Result<Self::Item, Self::TryRecvError> {
        self.rx.try_recv()
    }

    fn apply_update_item<'a>(
        &'a mut self,
        action: Self::Item,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>
    where
        Self::Item: 'a,
    {
        Box::pin(async move {
            let (seq, authority) = self.updater.queue(action);
            self.tx
                .safe_send(seq, authority.clone())
                .await
                .panic("failed to queue message in broadcast");
        })
    }

    fn update_ready(&mut self) -> bool {
        self.updater.update_ready()
    }

    fn update(&mut self) {
        self.updater.update();
    }
}

impl<D: DeterministicState> LeadWorker<D> {
    pub fn new(
        rx: Receiver<D::Action>,
        updater: FlushedUpdater<D>,
    ) -> (Self, SequencedReceiver<D::AuthorityAction>) {
        let (ch_tx, ch_rx) = channel(2048);

        let seq = updater.accept_seq();
        let seq_tx = SequencedSender::new(seq, ch_tx);
        let seq_rx = SequencedReceiver::new(seq, ch_rx);

        (
            LeadWorker {
                updater: updater.into_lead(),
                rx,
                tx: seq_tx,
            },
            seq_rx,
        )
    }

    pub fn into_flushed(self) -> FlushedUpdater<D> {
        self.updater.into_flushed()
    }
}
