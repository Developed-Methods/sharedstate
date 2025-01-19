use std::{fmt::Debug, panic::Location, time::Duration, future::Future};

use crate::message_io::{read_message_opt, send_message, ReadMessageError};
use super::io::SyncIO;

pub use client::*;
pub use server::*;

mod client;
mod server;
pub mod messages;

#[derive(Debug)]
pub enum HandshakeError {
    SendError(std::io::Error),
    ReadError(ReadMessageError),
    ConnectionRejected,
    UnexpectedResponse(messages::HandshakeMessage),
    UnexpectedEmptyMessage,
    InvalidStateIdOrSequence,
    RelayLoopDetected,
    RecoverError(&'static str),
}

trait MessageHelper: SyncIO {
    /* cast types */
    #[track_caller]
    fn send(buffer: &mut Vec<u8>, msg: &messages::HandshakeMessage, out: &mut Self::Write, progress_timeout: Duration) -> impl Future<Output = Result<(), std::io::Error>> {
        let caller = Location::caller();
        tracing::info!("send(/{}:{}): {:?}", caller.file().rsplit("/").next().unwrap(), caller.line(), msg);
        send_message(buffer, msg, out, progress_timeout)
    }

    #[track_caller]
    fn read_msg(buffer: &mut Vec<u8>, read: &mut Self::Read, progress_timeout: Duration) -> impl Future<Output = Result<Option<messages::HandshakeMessage>, ReadMessageError>> {
        let caller = Location::caller();

        async move {
            let res = read_message_opt(buffer, read, progress_timeout, Some(progress_timeout * 2)).await?;
            tracing::info!("received(/{}:{}): {:?}", caller.file().rsplit("/").next().unwrap(), caller.line(), res);
            Ok(res)
        }
    }
}

impl<T: SyncIO> MessageHelper for T {
}



#[cfg(test)]
mod test {
    use futures_util::task::ArcWake;
    use tokio::sync::mpsc::channel;

    use super::*;
    use crate::{recoverable_state::{RecoverableState, RecoverableStateAction}, testing::{setup_logging, state_tests::{TestState, TestStateAction}, test_sync_io::*}};

    #[tokio::test]
    async fn handshake_simple_test() {
        setup_logging();

        let io = TestSyncNet::new();

        let a_io = io.create(1).await.unwrap();
        let b_io = io.create(2).await.unwrap();

        let cleint = HandshakeClient::new(a_io.connect(&2).await.unwrap());
        let server = HandshakeServer::new(b_io.next_client().await.unwrap());

        let client = tokio::spawn(cleint.connect());

        let server = server.accept().await.unwrap()
            .send_state(&RecoverableState::new(193, TestState::default())).await.unwrap();

        let ConnectionEstablished::FreshConnection(client) = client.await.unwrap().unwrap() else { panic!() };
        let (client, state) = client.load_state::<TestState>().await.unwrap();

        assert_eq!(state.details().id, 193);
        let (_, mut client) = client.start_io_workers();

        let (b_tx, mut server_rx) = channel(1024);
        let (_, mut server_tx) = server.start_io_tasks(b_tx);

        server_tx.send((110, TestStateAction::Add { slot: 1, value: 99 })).await.unwrap();

        assert_eq!(
            client.authority_rx.recv().await.unwrap(),
            (1, RecoverableStateAction::StateAction((110, TestStateAction::Add { slot: 1, value: 99 })))
        );

        client.action_tx.send(TestStateAction::Add { slot: 0, value: 123 }).await.unwrap();
        assert_eq!(server_rx.recv().await.unwrap(), TestStateAction::Add { slot: 0, value: 123 });
    }
}

