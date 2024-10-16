use std::{fmt::Debug, future::Future, hash::Hash};
use message_encoding::MessageEncoding;
use tokio::io::{AsyncRead, AsyncWrite};

pub trait SyncIO: Sized + Send + Sync + 'static {
    type Address: MessageEncoding + Debug + Clone + Send + Sync + PartialEq + Eq + Hash + 'static;
    type Read: AsyncRead + Send + Unpin + 'static;
    type Write: AsyncWrite + Send + Unpin + 'static;

    fn connect(&self, remote: &Self::Address) -> impl Future<Output = std::io::Result<SyncConnection<Self>>> + Send;

    fn next_client(&self) -> impl Future<Output = std::io::Result<SyncConnection<Self>>> + Send;
}

pub struct SyncConnection<I: SyncIO> {
    pub remote: I::Address,
    pub read: I::Read,
    pub write: I::Write,
}

