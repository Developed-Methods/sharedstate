use sequenced_broadcast::{SequencedReceiver, SequencedSender, SequencedSenderError};
use tokio::sync::{mpsc::{error::TryRecvError, Receiver, Sender, channel}, oneshot};
use tokio_util::sync::CancellationToken;
use std::{future::Future, marker::PhantomData, sync::Arc};

use crate::{recoverable_state::RecoverableStateAction, state::DeterministicState, utils::PanicHelper};

pub struct MessageRelay<M: MessageIO> {
    next_request_tx: Sender<NextRequest<M>>,
    cancel: Arc<CancellationToken>,
    _phantom: PhantomData<M>,
}

impl<M: MessageIO> MessageRelay<M> {
    pub fn new(input: M::Receiver, output: M::Sender) -> Self {
        let (tx, rx) = channel(32);

        let cancel = Arc::new(CancellationToken::new());

        tokio::spawn(Worker {
            output,
            input,
            input_item: None,
            next_input_rx: rx,
            next_request: None,
            cancel: cancel.clone(),
            input_closed: false,
        }.start());

        MessageRelay {
            next_request_tx: tx,
            cancel,
            _phantom: PhantomData,
        }
    }

    pub async fn replace_input(&mut self, input: M::Receiver, wait_for_close: bool) -> Result<M::Receiver, M::Receiver> {
        let (tx, rx) = oneshot::channel();

        self.next_request_tx.send(NextRequest::ChangeInput(ChangeInputRequest {
            wait_for_close,
            rx: input,
            callback: tx,
        })).await.map_err(|e| {
            let NextRequest::ChangeInput(i) = e.0 else { panic!() };
            i.rx
        })?;

        rx.await.panic("worker closed")
    }

    pub async fn replace_output(&mut self, output: M::Sender) -> Result<M::Sender, M::Sender> {
        let (tx, rx) = oneshot::channel();

        self.next_request_tx.send(NextRequest::ChangeOutput(ChangeOutputRequest {
            tx: output,
            callback: tx,
        })).await.map_err(|e| {
            let NextRequest::ChangeOutput(i) = e.0 else { panic!() };
            i.tx
        })?;

        rx.await.panic("worker closed")
    }
}

impl<M: MessageIO> Drop for MessageRelay<M> {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

pub trait MessageIO: Default + 'static {
    type Message: Send;
    type Receiver: Send;
    type Sender: Send;

    fn send(tx: &mut Self::Sender, item: Self::Message) -> impl Future<Output = Result<(), Self::Message>> + Send;

    fn receive(rx: &mut Self::Receiver) -> impl Future<Output = Option<Self::Message>> + Send;

    fn can_replace_rx(current: &Self::Receiver, option: &Self::Receiver) -> bool {
        true
    }

    fn can_replace_tx(current: &Self::Sender, option: &Self::Sender) -> bool {
        true
    }

    fn try_receive(rx: &mut Self::Receiver) -> Option<Self::Message>;

    fn is_tx_closed(tx: &Self::Sender) -> bool;
}

pub struct SequencedMessages<T>(PhantomData<T>);

impl<T> Default for SequencedMessages<T> {
    fn default() -> Self {
        SequencedMessages(PhantomData)
    }
}

impl<T: Send + 'static> MessageIO for SequencedMessages<T> {
    type Message = (u64, T);
    type Receiver = SequencedReceiver<T>;
    type Sender = SequencedSender<T>;

    async fn send(tx: &mut Self::Sender, item: Self::Message) -> Result<(), Self::Message> {
        match tx.safe_send(item.0, item.1).await {
            Ok(_) => Ok(()),
            Err(SequencedSenderError::InvalidSequence(expected, _)) => panic!("invalid sequence, expected: {}, but got: {}", expected, item.0),
            Err(SequencedSenderError::ChannelClosed(r)) => Err((item.0, r)),
        }
    }

    async fn receive(rx: &mut Self::Receiver) -> Option<Self::Message> {
        rx.recv().await
    }

    fn is_tx_closed(tx: &Self::Sender) -> bool {
        tx.is_closed()
    }

    fn can_replace_rx(current: &Self::Receiver, option: &Self::Receiver) -> bool {
        current.next_seq() == option.next_seq()
    }

    fn can_replace_tx(current: &Self::Sender, option: &Self::Sender) -> bool {
        current.seq() == option.seq()
    }

    fn try_receive(rx: &mut Self::Receiver) -> Option<Self::Message> {
        rx.try_recv().ok()
    }
}

pub struct MpscMessages<T>(PhantomData<T>);

impl<T> Default for MpscMessages<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: Send + 'static> MessageIO for MpscMessages<T> {
    type Message = T;
    type Receiver = Receiver<T>;
    type Sender = Sender<T>;

    async fn send(tx: &mut Self::Sender, item: Self::Message) -> Result<(), Self::Message> {
        tx.send(item).await.map_err(|e| e.0)
    }

    async fn receive(rx: &mut Self::Receiver) -> Option<Self::Message> {
        rx.recv().await
    }

    fn is_tx_closed(tx: &Self::Sender) -> bool {
        tx.is_closed()
    }

    fn can_replace_rx(_current: &Self::Receiver, _option: &Self::Receiver) -> bool {
        true
    }

    fn try_receive(rx: &mut Self::Receiver) -> Option<Self::Message> {
        rx.try_recv().ok()
    }
}

pub struct RecoverableActionMessages<T>(PhantomData<T>);

impl<T> Default for RecoverableActionMessages<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<A: Send + 'static> MessageIO for RecoverableActionMessages<A> {
    type Message = A;
    type Receiver = Receiver<A>;
    type Sender = Sender<RecoverableStateAction<A>>;

    async fn send(tx: &mut Self::Sender, item: Self::Message) -> Result<(), Self::Message> {
        tx.send(RecoverableStateAction::StateAction(item)).await.map_err(|e| match e.0 {
            RecoverableStateAction::StateAction(a) => a,
            _ => panic!(),
        })
    }

    async fn receive(rx: &mut Self::Receiver) -> Option<Self::Message> {
        rx.recv().await
    }

    fn is_tx_closed(tx: &Self::Sender) -> bool {
        tx.is_closed()
    }

    fn can_replace_rx(_current: &Self::Receiver, _option: &Self::Receiver) -> bool {
        true
    }

    fn try_receive(rx: &mut Self::Receiver) -> Option<Self::Message> {
        rx.try_recv().ok()
    }
}

struct Worker<M: MessageIO> {
    output: M::Sender,
    input: M::Receiver,
    input_closed: bool,
    input_item: Option<M::Message>,
    next_request: Option<NextRequest<M>>,
    next_input_rx: Receiver<NextRequest<M>>,
    cancel: Arc<CancellationToken>,
}

enum NextRequest<M: MessageIO> {
    ChangeInput(ChangeInputRequest<M>),
    ChangeOutput(ChangeOutputRequest<M>),
}

struct ChangeInputRequest<M: MessageIO> {
    wait_for_close: bool,
    rx: M::Receiver,
    callback: oneshot::Sender<Result<M::Receiver, M::Receiver>>,
}

struct ChangeOutputRequest<M: MessageIO> {
    tx: M::Sender,
    callback: oneshot::Sender<Result<M::Sender, M::Sender>>,
}

impl<M: MessageIO> Worker<M> {
    async fn start(mut self) {
        loop {
            /* read from input */
            {
                if self.input_item.is_none() {
                    self.input_item = M::try_receive(&mut self.input);
                }

                let mut remaining = 1024;

                while let Some(item) = self.input_item.take() {
                    if let Err(failed) = M::send(&mut self.output, item).await {
                        self.input_item = Some(failed);
                        break;
                    }

                    if remaining <= 1 {
                        break;
                    }

                    remaining -= 1;
                    self.input_item = M::try_receive(&mut self.input);
                }
            }

            /* handle next request */
            {
                if self.next_request.is_none() {
                    self.next_request = match self.next_input_rx.try_recv() {
                        Ok(v) => Some(v),
                        Err(TryRecvError::Disconnected) => {
                            tracing::info!("MessageRelay dropped, closing worker");
                            return;
                        },
                        Err(TryRecvError::Empty) => None,
                    };
                }

                /* if we're blocked, wait for next request */
                if (self.input_closed || M::is_tx_closed(&self.output)) && self.next_request.is_none() {
                    tokio::select! {
                        next = self.next_input_rx.recv() => {
                            self.next_request = next;
                        }
                        _ = self.cancel.cancelled() => {
                            tracing::info!("MessageRelay dropped, ending worker");
                            return;
                        }
                    }

                    if self.next_request.is_none() {
                        continue;
                    }
                }

                while let Some(next) = self.next_request.take() {
                    match next {
                        NextRequest::ChangeInput(next) => {
                            if next.wait_for_close && !self.input_closed {
                                self.next_request = Some(NextRequest::ChangeInput(next));
                                break;
                            }

                            self.next_request = self.next_input_rx.try_recv().ok();

                            if !M::can_replace_rx(&self.input, &next.rx) {
                                let _ = next.callback.send(Err(next.rx));
                                continue;
                            }

                            let _ = next.callback.send(Ok(std::mem::replace(&mut self.input, next.rx)));
                            self.input_closed = false;
                        }
                        NextRequest::ChangeOutput(next) => {
                            self.next_request = self.next_input_rx.try_recv().ok();

                            if !M::can_replace_tx(&self.output, &next.tx) {
                                let _ = next.callback.send(Err(next.tx));
                                continue;
                            }

                            let _ = next.callback.send(Ok(std::mem::replace(&mut self.output, next.tx)));
                        }
                    }
                }
            }

            if self.next_request.is_none() {
                tokio::select! {
                    next_input = self.next_input_rx.recv() => {
                        self.next_request = next_input;
                    }
                    input_item = M::receive(&mut self.input) => {
                        self.input_item = input_item;
                        self.input_closed = self.input_item.is_none();
                    }
                    _ = self.cancel.cancelled() => {
                        return;
                    }
                }
            } else {
                tokio::select! {
                    input_item = M::receive(&mut self.input) => {
                        self.input_item = input_item;
                        self.input_closed = self.input_item.is_none();
                    }
                    _ = self.cancel.cancelled() => {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use rand_chacha::rand_core::{RngCore, SeedableRng};

    use crate::{testing::setup_logging, utils::TimeoutPanicHelper};

    use super::*;

    #[tokio::test]
    async fn message_relay_replace_with_no_drops_test() {
        setup_logging();

        let (in_tx, in_rx) = channel(1024);
        let (out_tx, out_rx) = channel(1024);

        let mut relay = MessageRelay::<MpscMessages<u64>>::new(in_rx, out_tx);

        let (inputs_tx, mut inputs_rx) = channel(32);
        let (outputs_tx, mut outputs_rx) = channel(32);

        inputs_tx.send(in_tx).await.unwrap();
        outputs_tx.send(out_rx).await.unwrap();

        let sender_task = tokio::spawn(async move {
            let mut i = 0;
            let mut input = inputs_rx.recv().await.panic("failed to get first input_tx");

            loop {
                if input.send(i).await.is_err() {
                    tracing::info!("input closed, ending sender test task");
                    break;
                }

                i += 1;

                if let Ok(next) = inputs_rx.try_recv() {
                    tracing::info!("replace input for {}", i);
                    input = next;
                }

            }

            i
        });

        let receiver_task = tokio::spawn(async move {
            let mut i = 0;

            while let Some(mut output) = outputs_rx.recv().await {
                while let Some(num) = output.recv().await {
                    assert_eq!(num, i);
                    i += 1;
                }
                tracing::info!("new output for {}", i);
            }

            i
        });

        let mut rand = rand_chacha::ChaCha20Rng::seed_from_u64(100);

        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(10)).await;

            let rng = rand.next_u32() % 3;
            if rng == 0 {
                let (in_tx, in_rx) = channel(1024);

                inputs_tx.send(in_tx).await
                    .panic("failed to send next input replacement");

                relay.replace_input(in_rx, true)
                    .timeout(Duration::from_millis(10), "failed to replace input").await
                    .panic("failed to replace");
            } else if rng == 1 {
                let (out_tx, out_rx) = channel(1024);

                outputs_tx.send(out_rx).await.panic("");

                relay.replace_output(out_tx)
                    .timeout(Duration::from_millis(10), "failed to replace output").await
                    .panic("failed to replace");
            }
        }

        tracing::info!("done with replacements, ending test");

        drop(relay);
        drop(outputs_tx);

        let send_num = sender_task.await.unwrap();
        let receive_num = receiver_task.await.unwrap();

        tracing::info!("done send: {}, received: {}", send_num, receive_num);
        assert!(send_num - receive_num <= 1024.max(send_num / 10_000));
    }

//     use crate::utils::TimeoutPanicHelper;
// 
//     use super::*;
// 
//     #[tokio::test]
//     async fn message_relay_test() {
//         let (in_tx, in_rx) = channel(1024);
//         let (out_tx, mut out_rx) = channel(1024);
// 
//         let relay = MessageRelay::<&'static str>::new(in_rx, RelayOutput::Sender(out_tx));
// 
//         in_tx.send("Hello WOrld").test_timeout().await.unwrap();
//         assert_eq!(out_rx.recv().test_timeout().await.unwrap(), "Hello WOrld");
// 
//         let (sequenced_tx, mut sequenced_rx) = channel(1024);
//         let mut old_sender = relay.replace_output(RelayOutput::SequencedSender(SequencedSender::new(100, sequenced_tx)))
//             .test_timeout().await.unwrap()
//             .test_timeout().await.unwrap();
// 
//         in_tx.send("new message").test_timeout().await.unwrap();
//         out_rx.recv().expect_timeout().await;
// 
//         old_sender.send("out of bound").test_timeout().await.unwrap();
//         assert_eq!(out_rx.recv().test_timeout().await.unwrap(), "out of bound");
// 
//         assert_eq!(sequenced_rx.recv().test_timeout().await.unwrap(), (100, "new message"));
//     }
}

