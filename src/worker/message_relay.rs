use std::{future::{poll_fn, Future}, pin::Pin, task::Poll};

use sequenced_broadcast::{SequencedSender, SequencedSenderPermit};
use tokio::sync::{mpsc::{channel, error::{SendError, TryRecvError, TrySendError}, Permit, Receiver, Sender}, oneshot};

use crate::utils::PanicHelper;

pub struct MessageRelaySender<T> {
    relay: MessageRelay<T>,
    tx: Sender<T>,
}

impl<T> Clone for MessageRelaySender<T> {
    fn clone(&self) -> Self {
        MessageRelaySender {
            relay: self.relay.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl<T: Send + 'static> MessageRelaySender<T> {
    pub fn new(output: RelayOutput<T>) -> Self {
        let (tx, rx) = channel(1024);

        MessageRelaySender {
            relay: MessageRelay::new(rx, output),
            tx,
        }
    }

    pub fn sender(&self) -> Sender<T> {
        self.tx.clone()
    }

    pub async fn send(&self, item: T) -> Result<(), T> {
        self.tx.send(item).await.map_err(|e| e.0)
    }

    pub async fn replace_output(&self, output: RelayOutput<T>) -> RelayOutput<T> {
        let old_rx = self.relay.replace_output(output).await
            .panic("input closed, should not be as we have reference");
        old_rx.await.panic("worker dropped response")
    }
}

pub struct MessageRelay<T> {
    next_sender: Sender<(RelayOutput<T>, oneshot::Sender<RelayOutput<T>>)>,
}

impl<T> Clone for MessageRelay<T> {
    fn clone(&self) -> Self {
        MessageRelay {
            next_sender: self.next_sender.clone(),
        }
    }
}

pub enum RelayOutput<T> {
    Sender(Sender<T>),
    SequencedSender(SequencedSender<T>),
}

impl<T> RelayOutput<T> {
    pub fn is_closed(&self) -> bool {
        match self {
            Self::Sender(s) => s.is_closed(),
            Self::SequencedSender(s) => s.is_closed(),
        }
    }

    pub async fn send(&mut self, item: T) -> Result<(), SendError<T>> {
        match self {
            Self::Sender(s) => s.send(item).await,
            Self::SequencedSender(s) => s.send(item).await.map_err(|e| SendError(e.into_inner())),
        }
    }

    pub fn try_send(&mut self, item: T) -> Result<(), TrySendError<T>> {
        match self {
            Self::Sender(s) => s.try_send(item),
            Self::SequencedSender(s) => s.try_send(item),
        }
    }

    pub async fn reserve(&mut self) -> Result<RelayOutputReserved<T>, SendError<()>> {
        match self {
            Self::Sender(s) => s.reserve().await.map(RelayOutputReserved::Sender),
            Self::SequencedSender(s) => s.reserve().await.map(RelayOutputReserved::SequencedSender),
        }
    }
}

pub enum RelayOutputReserved<'a, T> {
    Sender(Permit<'a, T>),
    SequencedSender(SequencedSenderPermit<'a, T>),
}

impl<'a, T> RelayOutputReserved<'a, T> {
    pub fn send(self, item: T) {
        match self {
            Self::Sender(s) => s.send(item),
            Self::SequencedSender(s) => s.send(item),
        }
    }
}

impl<T: Send + 'static> MessageRelay<T> {
    pub fn new(input: Receiver<T>, output: RelayOutput<T>) -> Self {
        let (tx, rx) = channel(1);

        tokio::spawn(MessageRelayWorker {
            input,
            output,
            next_sender_rx: rx,
            next_sender: None,
            next_msg: None,
        }.start());

        MessageRelay {
            next_sender: tx,
        }
    }
    
    pub async fn replace_output(&self, output: RelayOutput<T>) -> Option<oneshot::Receiver<RelayOutput<T>>> {
        let (tx, rx) = oneshot::channel();
        self.next_sender.send((output, tx)).await.ok()?;
        Some(rx)
    }

    pub fn is_input_closed(&self) -> bool {
        self.next_sender.is_closed()
    }
}

struct MessageRelayWorker<T> {
    input: Receiver<T>,
    output: RelayOutput<T>,
    next_sender_rx: Receiver<(RelayOutput<T>, oneshot::Sender<RelayOutput<T>>)>,
    next_sender: Option<(RelayOutput<T>, oneshot::Sender<RelayOutput<T>>)>,
    next_msg: Option<T>,
}

impl<T: Send + 'static> MessageRelayWorker<T> {
    async fn start(mut self) {
        loop {
            tokio::task::yield_now().await;

            let mut new_senders_closed = false;
            if self.next_sender.is_none() {
                self.next_sender = match self.next_sender_rx.try_recv() {
                    Ok(details) => Some(details),
                    Err(TryRecvError::Empty) => None,
                    Err(TryRecvError::Disconnected) => {
                        new_senders_closed = true;
                        None
                    }
                };

                if self.next_sender.is_none() && self.output.is_closed() {
                    self.next_sender = self.next_sender_rx.recv().await;
                    if self.next_sender.is_none() {
                        tracing::info!("MessageRelayWorker: output and next_sender_rx is closed");
                        return;
                    }
                }
            }

            if let Some(next) = self.next_sender.take() {
                let replaced = std::mem::replace(&mut self.output, next.0);
                let _ = next.1.send(replaced);
            }

            if self.next_msg.is_none() {
                self.next_msg = match self.input.try_recv() {
                    Ok(msg) => Some(msg),
                    Err(TryRecvError::Empty) => None,
                    Err(TryRecvError::Disconnected) => {
                        tracing::info!("MessageRelayWorker: input is closed");
                        return;
                    }
                };
            }

            /* flush input to output */
            {
                let mut remaining = 1024;
                while let Some(msg) = self.next_msg.take() {
                    if let Err(error) = self.output.try_send(msg) {
                        self.next_msg = Some(error.into_inner());
                        break;
                    }

                    if remaining == 0 {
                        break;
                    }
                    remaining -= 1;

                    self.next_msg = self.input.try_recv().ok();
                }
            }

            let next_msg = &mut self.next_msg;
            let mut reserve = if self.output.is_closed() { None } else { Some(self.output.reserve()) };

            poll_fn(|cx| {
                let mut is_ready = false;

                if next_msg.is_some() {
                    if let Some(reserve) = &mut reserve {
                        let pinned = unsafe { Pin::new_unchecked(reserve) };

                        if let Poll::Ready(slot) = pinned.poll(cx) {
                            is_ready = true;
                            if let Ok(slot) = slot {
                                slot.send(next_msg.take().unwrap());
                            }
                        }
                    }
                }
                else if let Poll::Ready(item) = self.input.poll_recv(cx) {
                    *next_msg = item;
                    is_ready = true;
                }

                if !new_senders_closed {
                    if let Poll::Ready(item) = self.next_sender_rx.poll_recv(cx) {
                        self.next_sender = item;
                        is_ready = true;
                    }
                }

                if is_ready {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }).await;
        }
    }
}

#[cfg(test)]
mod test {
    use crate::utils::TimeoutPanicHelper;

    use super::*;

    #[tokio::test]
    async fn message_relay_test() {
        let (in_tx, in_rx) = channel(1024);
        let (out_tx, mut out_rx) = channel(1024);

        let relay = MessageRelay::<&'static str>::new(in_rx, RelayOutput::Sender(out_tx));

        in_tx.send("Hello WOrld").test_timeout().await.unwrap();
        assert_eq!(out_rx.recv().test_timeout().await.unwrap(), "Hello WOrld");

        let (sequenced_tx, mut sequenced_rx) = channel(1024);
        let mut old_sender = relay.replace_output(RelayOutput::SequencedSender(SequencedSender::new(100, sequenced_tx)))
            .test_timeout().await.unwrap()
            .test_timeout().await.unwrap();

        in_tx.send("new message").test_timeout().await.unwrap();
        out_rx.recv().expect_timeout().await;

        old_sender.send("out of bound").test_timeout().await.unwrap();
        assert_eq!(out_rx.recv().test_timeout().await.unwrap(), "out of bound");

        assert_eq!(sequenced_rx.recv().test_timeout().await.unwrap(), (100, "new message"));
    }
}

