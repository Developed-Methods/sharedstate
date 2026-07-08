use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use message_encoding::MessageEncoding;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    protocol::framing::{
        read_message_opt, send_close_message, send_message, send_zero_message, MessageSizeHeader, ReadMessageResult,
        DEFAULT_MAX_FRAME_SIZE,
    },
    transport::traits::SyncIO,
};

#[derive(Clone, Debug)]
pub struct NetIoSettings {
    pub process_timeout: Duration,
    pub message_timeout: Duration,
    pub max_frame_size: MessageSizeHeader,
}

impl Default for NetIoSettings {
    fn default() -> Self {
        Self {
            process_timeout: Duration::from_secs(2),
            message_timeout: Duration::from_secs(12),
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }
}

pub struct ReadChannel<I: SyncIO, M: MessageEncoding> {
    pub remote: I::Address,
    pub input: I::Read,
    pub output: Sender<M>,
    pub settings: NetIoSettings,
}

pub struct WriteChannel<I: SyncIO, M: MessageEncoding> {
    pub remote: I::Address,
    pub input: Receiver<M>,
    pub output: I::Write,
    pub settings: NetIoSettings,
}

impl<I: SyncIO, M: MessageEncoding + Send + Sync + 'static> ReadChannel<I, M> {
    pub async fn start(mut self) {
        let mut buffer = vec![0u8; 2048];

        loop {
            tokio::task::yield_now().await;

            let read_opt_res = tokio::select! {
                read_opt_res = read_message_opt::<M, _>(
                    &mut buffer,
                    &mut self.input,
                    self.settings.process_timeout,
                    Some(self.settings.message_timeout),
                    self.settings.max_frame_size,
                ) => read_opt_res,
                _ = self.output.closed() => {
                    tracing::info!(remote = ?self.remote, "output closed, stopping read");
                    break;
                }
            };

            match read_opt_res {
                Ok(ReadMessageResult::Message(msg)) => {
                    if self.output.send(msg).await.is_err() {
                        tracing::error!(remote = ?self.remote, "failed to send message to output, stopping read");
                        break;
                    }
                }
                Ok(ReadMessageResult::KeepAlive) => {
                    continue;
                }
                Ok(ReadMessageResult::Close) => {
                    tracing::info!(remote = ?self.remote, "remote closed connection");
                    break;
                }
                Err(error) => {
                    if error.is_disconnect() {
                        tracing::debug!(remote = ?self.remote, ?error, "network read closed");
                    } else {
                        tracing::error!(remote = ?self.remote, ?error, "failed to read from network");
                    }
                    break;
                }
            }
        }
    }
}

const MIN_MESSAGE_INTERVAL: Duration = Duration::from_millis(500);
const MAX_MESSAGE_INTERVAL: Duration = Duration::from_secs(2);
const MAX_WRITE_BATCH_MESSAGES: usize = 256;

struct DeferredFlush<'a, W>(&'a mut W);

impl<W: AsyncWrite + Unpin> AsyncWrite for DeferredFlush<'_, W> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut *self.0).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.0).poll_shutdown(cx)
    }
}

impl<I: SyncIO, M: MessageEncoding + Send + Sync + 'static> WriteChannel<I, M> {
    pub async fn start(mut self) {
        let mut buffer = vec![0u8; 2048];
        let keep_alive_msg_timeout =
            (self.settings.message_timeout / 3).clamp(MIN_MESSAGE_INTERVAL, MAX_MESSAGE_INTERVAL);

        loop {
            tokio::task::yield_now().await;

            let msg = tokio::select! {
                msg_opt = self.input.recv() => {
                    match msg_opt {
                        Some(v) => Some(v),
                        None => {
                            tracing::info!(remote = ?self.remote, "input closed, closing write");
                            let close_res = tokio::time::timeout(
                                self.settings.process_timeout,
                                send_close_message(&mut self.output),
                            )
                            .await;

                            match close_res {
                                Ok(Ok(())) => {}
                                Ok(Err(error)) => {
                                    tracing::debug!(remote = ?self.remote, ?error, "failed to send close message")
                                }
                                Err(error) => {
                                    tracing::debug!(remote = ?self.remote, ?error, "timed out sending close message")
                                }
                            }

                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(keep_alive_msg_timeout) => None,
            };

            let send_res = match msg {
                Some(msg) => {
                    tokio::time::timeout(
                        self.settings.process_timeout,
                        send_message_batch(
                            &mut buffer,
                            &mut self.input,
                            &mut self.output,
                            msg,
                            self.settings.process_timeout,
                            self.settings.max_frame_size,
                        ),
                    )
                    .await
                }
                None => tokio::time::timeout(self.settings.process_timeout, send_zero_message(&mut self.output)).await,
            };

            match send_res {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::error!(remote = ?self.remote, ?error, "failed to send message, closing write");
                    break;
                }
                Err(error) => {
                    tracing::error!(remote = ?self.remote, ?error, "timed out sending message, closing write");
                    break;
                }
            }
        }

        let shutdown_res = tokio::time::timeout(self.settings.process_timeout, self.output.shutdown()).await;
        match shutdown_res {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::debug!(remote = ?self.remote, ?error, "failed to shutdown write"),
            Err(error) => tracing::debug!(remote = ?self.remote, ?error, "timed out shutting down write"),
        }
    }
}

async fn send_message_batch<W, M>(
    buffer: &mut Vec<u8>,
    input: &mut Receiver<M>,
    output: &mut W,
    first: M,
    process_timeout: Duration,
    max_frame_size: MessageSizeHeader,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
    M: MessageEncoding,
{
    {
        let mut deferred = DeferredFlush(output);
        send_message(buffer, &first, &mut deferred, process_timeout, max_frame_size).await?;

        for _ in 1..MAX_WRITE_BATCH_MESSAGES {
            let Ok(next) = input.try_recv() else {
                break;
            };
            send_message(buffer, &next, &mut deferred, process_timeout, max_frame_size).await?;
        }
    }

    output.flush().await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        io::{duplex, AsyncReadExt, DuplexStream},
        sync::mpsc,
    };

    use super::*;
    use crate::{
        protocol::framing::{send_close_message, MessageSizeHeader},
        transport::traits::{SyncConnection, SyncIO},
    };

    struct DuplexSyncIo;

    impl SyncIO for DuplexSyncIo {
        type Address = u64;
        type Read = DuplexStream;
        type Write = DuplexStream;

        async fn connect(&self, _remote: &Self::Address) -> std::io::Result<SyncConnection<Self>> {
            Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "test io cannot connect"))
        }
    }

    fn test_settings() -> NetIoSettings {
        NetIoSettings {
            process_timeout: Duration::from_secs(1),
            message_timeout: Duration::from_secs(1),
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    #[tokio::test]
    async fn write_channel_sends_close_when_input_closes() {
        let (output, mut peer_read) = duplex(64);
        let (tx, rx) = mpsc::channel::<u64>(1);
        drop(tx);

        let handle = tokio::spawn(
            WriteChannel::<DuplexSyncIo, u64> {
                remote: 1,
                input: rx,
                output,
                settings: test_settings(),
            }
            .start(),
        );

        let mut header = [0; std::mem::size_of::<MessageSizeHeader>()];
        peer_read.read_exact(&mut header).await.unwrap();
        handle.await.unwrap();

        assert_eq!(header, MessageSizeHeader::MAX.to_be_bytes());
    }

    #[tokio::test]
    async fn read_channel_stops_without_message_on_close_frame() {
        let (input, mut peer_write) = duplex(64);
        let (tx, mut rx) = mpsc::channel::<u64>(1);

        let handle = tokio::spawn(
            ReadChannel::<DuplexSyncIo, u64> {
                remote: 1,
                input,
                output: tx,
                settings: test_settings(),
            }
            .start(),
        );

        send_close_message(&mut peer_write).await.unwrap();
        handle.await.unwrap();

        assert!(rx.recv().await.is_none());
    }
}
