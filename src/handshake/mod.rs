use std::{fmt::Debug, panic::Location, time::Duration, future::Future};

use message_encoding::MessageEncoding;

use crate::message_io::{read_message_opt, send_message, ReadMessageError};
use super::io::SyncIO;

pub use client::*;
pub use server::*;

mod client;
mod server;
pub mod messages;

#[derive(Debug)]
pub enum HandshakeError<A: MessageEncoding + Debug> {
    SendError(std::io::Error),
    ReadError(ReadMessageError),
    UnexpectedResponse(messages::HandshakeMessage<A>),
    UnexpectedEmptyMessage,
    InvalidStateIdOrSequence,
    RelayLoopDetected,
    RecoverError(&'static str),
}

trait MessageHelper: SyncIO {
    /* cast types */
    #[track_caller]
    fn send(buffer: &mut Vec<u8>, msg: &messages::HandshakeMessage<Self::Address>, out: &mut Self::Write, progress_timeout: Duration) -> impl Future<Output = Result<(), std::io::Error>> {
        let caller = Location::caller();
        tracing::info!("send({}:{}): {:?}", caller.file().rsplit("/").next().unwrap(), caller.line(), msg);
        send_message(buffer, msg, out, progress_timeout)
    }

    #[track_caller]
    fn read_msg(buffer: &mut Vec<u8>, read: &mut Self::Read, progress_timeout: Duration) -> impl Future<Output = Result<Option<messages::HandshakeMessage<Self::Address>>, ReadMessageError>> {
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


