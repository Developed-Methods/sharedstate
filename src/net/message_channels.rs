use std::time::Duration;

use message_encoding::MessageEncoding;
use tokio::sync::mpsc::{Receiver, Sender};

use super::{io::SyncIO, message_io::{read_message_opt, send_message, send_zero_message}};

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
                Ok(Some(msg)) => {
                    if self.output.send(msg).await.is_err() {
                        tracing::error!("failed to send message to output, stopping read");
                        break;
                    }
                }
                Ok(None) => {
                    continue;
                }
                Err(error) => {
                    tracing::error!(?error, "failed to read from network");
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
            let msg = tokio::select! {
                msg_opt = self.input.recv() => {
                    match msg_opt {
                        Some(v) => Some(v),
                        None => {
                            tracing::info!("input closed, ending write");
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
                        send_message(&mut buffer, &msg, &mut self.output, self.settings.process_timeout)
                    ).await
                }
                None => {
                    tokio::time::timeout(
                        self.settings.process_timeout,
                        send_zero_message(&mut self.output)
                    ).await
                }
            };

            if let Err(error) = send_res {
                tracing::error!(?error, "failed to send message, closing write");
                break;
            }
        }
    }
}
