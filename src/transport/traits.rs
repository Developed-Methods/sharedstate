use message_encoding::MessageEncoding;
use std::{fmt::Debug, future::Future, hash::Hash};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc::{channel, Receiver, Sender},
};

use crate::{
    protocol::messages::{SyncRequest, SyncResponse},
    state::deterministic::DeterministicState,
    transport::channels::{NetIoSettings, ReadChannel, WriteChannel},
};

pub trait SyncIOAddress:
    Debug + Clone + Copy + Send + Sync + PartialEq + Eq + Ord + Hash + MessageEncoding + 'static
{
}

impl<T: Debug + Clone + Copy + Send + Sync + PartialEq + Eq + Ord + Hash + MessageEncoding + 'static> SyncIOAddress
    for T
{
}

pub trait SyncIO: Sized + Send + Sync + 'static {
    type Address: SyncIOAddress;
    type Read: AsyncRead + Send + Unpin + 'static;
    type Write: AsyncWrite + Send + Unpin + 'static;

    fn connect(&self, remote: &Self::Address) -> impl Future<Output = std::io::Result<SyncConnection<Self>>> + Send;
}

pub trait SyncIOListener: SyncIO {
    fn next_client(&self) -> impl Future<Output = std::io::Result<SyncConnection<Self>>> + Send;
}

pub struct SyncConnection<I: SyncIO> {
    pub remote: I::Address,
    pub read: I::Read,
    pub write: I::Write,
}

pub type ClientChannels<I, D> = (
    <I as SyncIO>::Address,
    Sender<SyncRequest<<I as SyncIO>::Address, D>>,
    Receiver<SyncResponse<<I as SyncIO>::Address, D>>,
);

pub type ServerChannels<I, D> = (
    <I as SyncIO>::Address,
    Sender<SyncResponse<<I as SyncIO>::Address, D>>,
    Receiver<SyncRequest<<I as SyncIO>::Address, D>>,
);

impl<I: SyncIO> SyncConnection<I> {
    pub fn client_channels<D>(self, settings: NetIoSettings) -> ClientChannels<I, D>
    where
        D: DeterministicState + MessageEncoding,
        D::Action: MessageEncoding,
        D::AuthorityAction: MessageEncoding,
    {
        self.channels(settings)
    }

    pub fn server_channels<D>(self, settings: NetIoSettings) -> ServerChannels<I, D>
    where
        D: DeterministicState + MessageEncoding,
        D::Action: MessageEncoding,
        D::AuthorityAction: MessageEncoding,
    {
        self.channels(settings)
    }

    fn channels<W: MessageEncoding + Send + Sync + 'static, R: MessageEncoding + Send + Sync + 'static>(
        self,
        settings: NetIoSettings,
    ) -> (I::Address, Sender<W>, Receiver<R>) {
        let (write_tx, write_rx) = channel(512);
        let (read_tx, read_rx) = channel(512);

        tokio::spawn(
            WriteChannel::<I, W> {
                remote: self.remote,
                input: write_rx,
                output: self.write,
                settings: settings.clone(),
            }
            .start(),
        );

        tokio::spawn(
            ReadChannel::<I, R> {
                remote: self.remote,
                output: read_tx,
                input: self.read,
                settings,
            }
            .start(),
        );

        (self.remote, write_tx, read_rx)
    }
}
