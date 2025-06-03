use std::time::Duration;

use sequenced_broadcast::{SequencedReceiver, SequencedSender};
use tokio::sync::mpsc::{channel, Receiver};
use tokio_util::sync::CancellationToken;

use crate::state::{DeterministicState, FlushedUpdater, LeadUpdater};

use super::task_and_cancel::TaskAndCancel;

pub struct LeadWorker<D: DeterministicState> {
    updater: LeadUpdater<D>,
    rx: Receiver<D::Action>,
    tx: SequencedSender<D::AuthorityAction>,
}

impl<D: DeterministicState> LeadWorker<D> where D::AuthorityAction: Clone {
    pub fn spawn(mut self) -> TaskAndCancel<Self> {
        TaskAndCancel::spawn(|cancel| async move {
            self.run_until_cancelled(cancel).await;
            self
        })
    }

    pub async fn run_until_cancelled(&mut self, cancel: CancellationToken) {
        loop {
            let action = tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    if self.updater.update_ready() {
                        self.updater.update();
                    }
                    continue;
                }
                _ = cancel.cancelled() => break,
                msg_opt = self.rx.recv() => {
                    let Some(msg) = msg_opt else { break };
                    msg
                }
            };

            let (seq, authority) = self.updater.queue(action);
            self.tx.safe_send(seq, authority.clone()).await.unwrap();

            let mut remaining = 128;
            while let Ok(action) = self.rx.try_recv() {
                let (seq, authority) = self.updater.queue(action);
                self.tx.safe_send(seq, authority.clone()).await.unwrap();

                if remaining == 0 {
                    break;
                }
                remaining -= 1;
            }

            if self.updater.update_ready() {
                self.updater.update();
            }
        }

        /* apply pending messages */
        while let Ok(action) = self.rx.try_recv() {
            let (seq, authority) = self.updater.queue(action);
            self.tx.safe_send(seq, authority.clone()).await.unwrap();
        }

        if self.updater.update_ready() {
            self.updater.update();
        }
    }
}


impl<D: DeterministicState> LeadWorker<D> {
    pub fn new(rx: Receiver<D::Action>, updater: FlushedUpdater<D>) -> (Self, SequencedReceiver<D::AuthorityAction>) {
        let (ch_tx, ch_rx) = channel(2048);

        let seq = updater.next_sequence();
        let seq_tx = SequencedSender::new(seq, ch_tx);
        let seq_rx = SequencedReceiver::new(seq, ch_rx);

        (
            LeadWorker {
                updater: updater.into_lead(),
                rx,
                tx: seq_tx,
            },
            seq_rx
        )
    }

    pub fn into_flushed(self) -> FlushedUpdater<D> {
        self.updater.into_flushed()
    }
}

