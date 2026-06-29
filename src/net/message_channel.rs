use std::time::Duration;

use message_encoding::MessageEncoding;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{Receiver, Sender};

use super::{
    message_io::{read_message_opt, send_close_message, send_message, send_zero_message, ReadMessageResult},
    sync_io::SyncIO,
};

#[derive(Clone, Debug)]
pub struct NetIoSettings {
    pub process_timeout: Duration,
    pub message_timeout: Duration,
}

impl Default for NetIoSettings {
    fn default() -> Self {
        Self {
            process_timeout: Duration::from_secs(2),
            message_timeout: Duration::from_secs(12),
        }
    }
}

pub struct ReadChannel<I: SyncIO, M: MessageEncoding> {
    pub input: I::Read,
    pub output: Sender<M>,
    pub settings: NetIoSettings,
}

pub struct WriteChannel<I: SyncIO, M: MessageEncoding> {
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
                ) => read_opt_res,
                _ = self.output.closed() => {
                    tracing::info!("output closed, stopping read");
                    break;
                }
            };

            match read_opt_res {
                Ok(ReadMessageResult::Message(msg)) => {
                    if self.output.send(msg).await.is_err() {
                        tracing::error!("failed to send message to output, stopping read");
                        break;
                    }
                }
                Ok(ReadMessageResult::KeepAlive) => {
                    continue;
                }
                Ok(ReadMessageResult::Close) => {
                    tracing::info!("remote closed connection");
                    break;
                }
                Err(error) => {
                    if error.is_disconnect() {
                        tracing::debug!(?error, "network read closed");
                    } else {
                        tracing::error!(?error, "failed to read from network");
                    }
                    break;
                }
            }
        }
    }
}

impl<I: SyncIO, M: MessageEncoding + Send + Sync + 'static> WriteChannel<I, M> {
    pub async fn start(mut self) {
        let mut buffer = vec![0u8; 2048];
        let zero_msg_timeout = self.settings.process_timeout / 3;

        loop {
            tokio::task::yield_now().await;

            let msg = tokio::select! {
                msg_opt = self.input.recv() => {
                    match msg_opt {
                        Some(v) => Some(v),
                        None => {
                            tracing::info!("input closed, closing write");
                            let close_res = tokio::time::timeout(
                                self.settings.process_timeout,
                                send_close_message(&mut self.output),
                            )
                            .await;

                            match close_res {
                                Ok(Ok(())) => {}
                                Ok(Err(error)) => tracing::debug!(?error, "failed to send close message"),
                                Err(error) => tracing::debug!(?error, "timed out sending close message"),
                            }

                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(zero_msg_timeout) => None,
            };

            let send_res = match msg {
                Some(msg) => {
                    tokio::time::timeout(
                        self.settings.process_timeout,
                        send_message(&mut buffer, &msg, &mut self.output, self.settings.process_timeout),
                    )
                    .await
                }
                None => tokio::time::timeout(self.settings.process_timeout, send_zero_message(&mut self.output)).await,
            };

            match send_res {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::error!(?error, "failed to send message, closing write");
                    break;
                }
                Err(error) => {
                    tracing::error!(?error, "timed out sending message, closing write");
                    break;
                }
            }
        }

        let shutdown_res = tokio::time::timeout(self.settings.process_timeout, self.output.shutdown()).await;
        match shutdown_res {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::debug!(?error, "failed to shutdown write"),
            Err(error) => tracing::debug!(?error, "timed out shutting down write"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{future::Future, time::Duration};

    use tokio::{
        io::{duplex, AsyncReadExt, DuplexStream},
        sync::mpsc,
    };

    use super::*;
    use crate::net::{
        message_io::{send_close_message, MessageSizeHeader},
        sync_io::{SyncConnection, SyncIO},
    };

    struct DuplexSyncIo;

    impl SyncIO for DuplexSyncIo {
        type Address = u64;
        type Read = DuplexStream;
        type Write = DuplexStream;

        fn connect(
            &self,
            _remote: &Self::Address,
        ) -> impl Future<Output = std::io::Result<SyncConnection<Self>>> + Send {
            async { Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "test io cannot connect")) }
        }
    }

    fn test_settings() -> NetIoSettings {
        NetIoSettings {
            process_timeout: Duration::from_secs(1),
            message_timeout: Duration::from_secs(1),
        }
    }

    #[tokio::test]
    async fn write_channel_sends_close_when_input_closes() {
        let (output, mut peer_read) = duplex(64);
        let (tx, rx) = mpsc::channel::<u64>(1);
        drop(tx);

        let handle = tokio::spawn(
            WriteChannel::<DuplexSyncIo, u64> {
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
