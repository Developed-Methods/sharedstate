use std::time::Duration;

use sequenced_broadcast::{SequencedReceiver, SequencedSender};
use tokio::sync::mpsc::channel;
use tokio_util::sync::CancellationToken;

use crate::state::{DeterministicState, FlushedUpdater, FollowUpdater};

use super::task_and_cancel::TaskAndCancel;

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
        self.rx.next_seq() == self.updater.next_sequence()
    }
    
    pub fn try_into_valid(self) -> Result<ValidSequencedRxAndUpdater<D>, SequencedRxAndUpdater<D>> {
        if self.is_valid_pair() {
            Ok(ValidSequencedRxAndUpdater(self))
        } else {
            Err(self)
        }
    }
}

impl<D: DeterministicState> FollowWorker<D> where D::AuthorityAction: Clone {
    pub fn new(rx_and_updater: ValidSequencedRxAndUpdater<D>) -> (Self, SequencedReceiver<D::AuthorityAction>) {
        let (ch_tx, ch_rx) = channel(2048);

        let seq = rx_and_updater.0.updater.next_sequence();
        let seq_tx = SequencedSender::new(seq, ch_tx);
        let seq_rx = SequencedReceiver::new(seq, ch_rx);

        (
            FollowWorker {
                updater: rx_and_updater.0.updater.into_follow(),
                rx: rx_and_updater.0.rx,
                tx: seq_tx,
            },
            seq_rx
        )
    }

    pub fn spawn(mut self) -> TaskAndCancel<Self> {
        TaskAndCancel::spawn(|cancel| async move {
            self.run_till_canceled(cancel).await;
            self
        })
    }

    pub async fn run_till_canceled(&mut self, cancel: CancellationToken) {
        loop {
            let (seq, action) = tokio::select! {
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

            let authority = self.updater.queue(seq, action);
            self.tx.safe_send(seq, authority.clone()).await.unwrap();

            let mut remaining = 128;
            while let Ok((seq, action)) = self.rx.try_recv() {
                let authority = self.updater.queue(seq, action);
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
        while let Ok((seq, action)) = self.rx.try_recv() {
            let authority = self.updater.queue(seq, action);
            self.tx.safe_send(seq, authority.clone()).await.unwrap();
        }

        if self.updater.update_ready() {
            self.updater.update();
        }
    }

    pub fn into_flushed(self) -> FlushedUpdater<D> {
        self.updater.into_flushed()
    }
}

